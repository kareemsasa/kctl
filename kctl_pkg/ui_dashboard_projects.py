from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlencode

from .git import ensure_git_repo, get_project_git_detail, get_project_git_summary, get_repo_root
from .types import PlanError
from .ui_dashboard_support import _escape, _page_link, _render_collapsible_section


def projects_file_path(repo_path: Path) -> Path:
    return repo_path / ".kctl" / "dashboard-projects.json"


def load_tracked_projects(repo_path: Path) -> list[str]:
    path = projects_file_path(repo_path)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    projects: list[str] = []
    for item in data:
        if not isinstance(item, str) or not item.strip():
            continue
        projects.append(str(Path(item).expanduser().resolve()))
    return sorted(dict.fromkeys(projects))


def save_tracked_projects(repo_path: Path, projects: list[str]) -> None:
    path = projects_file_path(repo_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(dict.fromkeys(projects)), indent=2) + "\n")


def add_tracked_project(repo_path: Path, project_path: Path) -> None:
    ensure_git_repo(project_path)
    repo_root = get_repo_root(project_path)
    resolved = str(repo_root)
    projects = load_tracked_projects(repo_path)
    if resolved in projects:
        raise PlanError(f"Project already tracked: {resolved}")
    projects.append(resolved)
    save_tracked_projects(repo_path, projects)


def remove_tracked_project(repo_path: Path, project_path: str) -> None:
    normalized_target = str(Path(project_path).expanduser().resolve())
    projects = [path for path in load_tracked_projects(repo_path) if path != normalized_target]
    save_tracked_projects(repo_path, projects)


def _render_project_card(project_path: str, summary: dict[str, object]) -> str:
    name = Path(project_path).name
    detail_url = "/projects/detail?" + urlencode({"path": project_path})
    header = (
        "<div class='project-header'>"
        f"<a class='project-name' href='{_escape(detail_url)}' style='color:inherit;text-decoration:none'>{_escape(name)}</a>"
        "<form method='post' action='/actions/remove-project'>"
        f"<input type='hidden' name='project_path' value='{_escape(project_path)}'>"
        "<button type='submit'>Remove</button>"
        "</form>"
        "</div>"
        f"<div class='project-path'><code>{_escape(project_path)}</code></div>"
    )
    if not summary.get("available"):
        error = summary.get("error", "unavailable")
        git_html = f"<div class='git-unavailable'>{_escape(str(error))}</div>"
        return f"<div class='project-item' data-project='{_escape(project_path)}'>{header}<div class='project-git'>{git_html}</div></div>"

    badges: list[str] = []
    branch = summary.get("branch")
    if branch:
        badges.append(f"<span class='git-badge git-branch'>{_escape(str(branch))}</span>")

    dirty = summary.get("dirty")
    changed = summary.get("changed_count", 0)
    if dirty:
        label = f"{changed} changed file{'s' if changed != 1 else ''}" if changed else "dirty"
        badges.append(f"<span class='git-badge git-dirty'>{_escape(label)}</span>")
    elif dirty is not None:
        badges.append("<span class='git-badge git-clean'>clean</span>")

    ahead_behind = summary.get("ahead_behind")
    if isinstance(ahead_behind, (list, tuple)) and len(ahead_behind) == 2:
        ahead, behind = ahead_behind
        if ahead and behind:
            badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead}</span>")
            badges.append(f"<span class='git-badge git-behind'>&darr;{behind}</span>")
        elif ahead:
            badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead} ahead</span>")
        elif behind:
            badges.append(f"<span class='git-badge git-behind'>&darr;{behind} behind</span>")
        else:
            badges.append("<span class='git-badge git-synced'>in sync</span>")

    last_commit = summary.get("last_commit")
    if last_commit:
        badges.append(f"<span class='git-commit'>{_escape(str(last_commit))}</span>")

    git_html = "".join(badges)
    return f"<div class='project-item' data-project='{_escape(project_path)}'>{header}<div class='project-git'>{git_html}</div></div>"


def render_projects_page(app: object, *, action_message: str | None = None) -> str:
    tracked_projects = app.load_tracked_projects()
    tracked_json = json.dumps(tracked_projects)
    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
    summaries = {p: get_project_git_summary(Path(p)) for p in tracked_projects}
    project_items_html = "".join(
        _render_project_card(project_path, summaries[project_path])
        for project_path in tracked_projects
    ) or "<div class='empty'>No tracked projects yet.</div>"
    add_project_html = _render_collapsible_section(
        "Add Project",
        (
            "<div class='help' style='margin-top:8px'>Add a local git repository path to the tracked projects list.</div>"
            "<form method='post' action='/actions/add-project' id='add_project_form'>"
            "<label for='project_path'><strong>Project Path</strong></label>"
            "<input id='project_path' name='project_path' type='text' placeholder='/path/to/project' required>"
            "<div id='project_path_status' class='repo-check'></div>"
            "<div id='project_path_duplicate' class='repo-check'></div>"
            "<button type='submit' id='add_project_button'>Add Project</button>"
            "</form>"
        ),
    )
    body = (
        "<main class='single-column'>"
        "<div class='column'>"
        f"{notice_html}"
        f"{add_project_html}"
        "<section class='panel'>"
        "<div style='display:flex;align-items:center;justify-content:space-between'>"
        "<h2 style='margin:0'>Tracked Projects</h2>"
        "<button id='refresh_git_btn' type='button' style='font-size:0.85em'>Refresh</button>"
        "</div>"
        "<div class='help'>Local repo paths used for cross-project plan runs.</div>"
        f"<div id='projects_list'>{project_items_html}</div>"
        "</section>"
        "</div>"
        "</main>"
    )
    projects_script = f"""\
window.addEventListener('DOMContentLoaded', () => {{
  wireRepoCheck('project_path', 'project_path_status');
  const trackedProjects = {tracked_json};
  const input = document.getElementById('project_path');
  const dupStatus = document.getElementById('project_path_duplicate');
  const addButton = document.getElementById('add_project_button');
  if (input && dupStatus && addButton) {{
    let timer = null;
    async function checkDuplicate() {{
      const value = input.value.trim();
      if (!value) {{
        dupStatus.textContent = '';
        dupStatus.dataset.status = '';
        addButton.disabled = false;
        return;
      }}
      const params = new URLSearchParams({{ path: value }});
      const response = await fetch('/api/resolve-path?' + params.toString());
      const data = await response.json();
      const resolved = data.resolved || '';
      if (resolved && trackedProjects.includes(resolved)) {{
        dupStatus.dataset.status = 'empty';
        dupStatus.textContent = 'Already tracked: ' + resolved;
        addButton.disabled = true;
      }} else {{
        dupStatus.textContent = '';
        dupStatus.dataset.status = '';
        addButton.disabled = false;
      }}
    }}
    function scheduleCheck() {{
      clearTimeout(timer);
      timer = setTimeout(checkDuplicate, 150);
    }}
    input.addEventListener('input', scheduleCheck);
    checkDuplicate();
  }}

  function renderBadges(d) {{
    let h = '';
    if (d.branch) h += `<span class="git-badge git-branch">${{esc(d.branch)}}</span>`;
    if (d.dirty) {{
      const n = d.changed_count || 0;
      const label = n ? n + ' changed file' + (n !== 1 ? 's' : '') : 'dirty';
      h += `<span class="git-badge git-dirty">${{esc(label)}}</span>`;
    }} else if (d.dirty === false) {{
      h += '<span class="git-badge git-clean">clean</span>';
    }}
    const ab = d.ahead_behind;
    if (Array.isArray(ab) && ab.length === 2) {{
      const [ahead, behind] = ab;
      if (ahead && behind) {{
        h += `<span class="git-badge git-ahead">&uarr;${{ahead}}</span>`;
        h += `<span class="git-badge git-behind">&darr;${{behind}}</span>`;
      }} else if (ahead) {{
        h += `<span class="git-badge git-ahead">&uarr;${{ahead}} ahead</span>`;
      }} else if (behind) {{
        h += `<span class="git-badge git-behind">&darr;${{behind}} behind</span>`;
      }} else {{
        h += '<span class="git-badge git-synced">in sync</span>';
      }}
    }}
    if (d.last_commit) h += `<span class="git-commit">${{esc(d.last_commit)}}</span>`;
    return h;
  }}
  function esc(s) {{
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }}

  const refreshBtn = document.getElementById('refresh_git_btn');
  if (refreshBtn) {{
    refreshBtn.addEventListener('click', async () => {{
      refreshBtn.disabled = true;
      refreshBtn.textContent = 'Refreshing…';
      for (const project of trackedProjects) {{
        const card = document.querySelector(`[data-project="${{CSS.escape(project)}}"]`);
        if (!card) continue;
        const gitDiv = card.querySelector('.project-git');
        if (!gitDiv) continue;
        try {{
          const params = new URLSearchParams({{ path: project }});
          const resp = await fetch('/api/project-git-status?' + params.toString());
          const data = await resp.json();
          if (!data.available) {{
            gitDiv.innerHTML = `<span class="git-unavailable">${{esc(data.error || 'unavailable')}}</span>`;
          }} else {{
            gitDiv.innerHTML = renderBadges(data);
          }}
        }} catch (e) {{
          gitDiv.innerHTML = '<span class="git-unavailable">fetch error</span>';
        }}
      }}
      refreshBtn.disabled = false;
      refreshBtn.textContent = 'Refresh';
    }});
  }}
}});
"""
    return app._page_shell(active_nav="Projects", body=body, extra_script=projects_script)


def render_project_detail_page(app: object, project_path: str, *, action_message: str | None = None) -> str:
    resolved = str(Path(project_path).expanduser().resolve())
    tracked = app.load_tracked_projects()
    if resolved not in tracked:
        return app._page_shell(
            active_nav="Projects",
            body=(
                "<main class='single-column'><div class='column'>"
                "<a class='back-link' href='/projects'>&larr; All Projects</a>"
                f"<section class='panel'><div class='empty'>Project not tracked: {_escape(resolved)}</div></section>"
                "</div></main>"
            ),
        )
    detail = get_project_git_detail(Path(resolved))
    name = _escape(str(detail.get("name", Path(resolved).name)))

    if not detail.get("available"):
        return app._page_shell(
            active_nav="Projects",
            body=(
                "<main class='single-column'><div class='column'>"
                "<a class='back-link' href='/projects'>&larr; All Projects</a>"
                f"<section class='panel'><h2>{name}</h2>"
                f"<div class='git-unavailable'>{_escape(str(detail.get('error', 'unavailable')))}</div>"
                "</section></div></main>"
            ),
        )

    path_hidden = f"<input type='hidden' name='path' value='{_escape(resolved)}'>"
    message_html = ""
    if action_message:
        is_error = action_message.lower().startswith("error:") or action_message.lower().startswith("failed")
        cls = "action-error" if is_error else "action-ok"
        message_html = f"<div class='action-message {cls}'>{_escape(action_message)}</div>"

    branch = detail.get("branch")
    dirty = detail.get("dirty")
    changed = detail.get("changed_count", 0)
    ahead_behind = detail.get("ahead_behind")

    status_badges: list[str] = []
    if branch:
        status_badges.append(f"<span class='git-badge git-branch'>{_escape(str(branch))}</span>")
    if dirty:
        label = f"{changed} changed file{'s' if changed != 1 else ''}" if changed else "dirty"
        status_badges.append(f"<span class='git-badge git-dirty'>{_escape(label)}</span>")
    elif dirty is not None:
        status_badges.append("<span class='git-badge git-clean'>clean</span>")
    if isinstance(ahead_behind, (list, tuple)) and len(ahead_behind) == 2:
        ahead, behind = ahead_behind
        if ahead and behind:
            status_badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead}</span>")
            status_badges.append(f"<span class='git-badge git-behind'>&darr;{behind}</span>")
        elif ahead:
            status_badges.append(f"<span class='git-badge git-ahead'>&uarr;{ahead} ahead</span>")
        elif behind:
            status_badges.append(f"<span class='git-badge git-behind'>&darr;{behind} behind</span>")
        else:
            status_badges.append("<span class='git-badge git-synced'>in sync</span>")

    launch_session_url = _page_link("/sessions", project=resolved)
    header_html = (
        "<a class='back-link' href='/projects'>&larr; All Projects</a>"
        f"{message_html}"
        "<section class='panel'>"
        "<div class='detail-header'>"
        f"<h2 style='margin:0'>{name}</h2>"
        "<div style='display:flex;gap:8px;align-items:center'>"
        f"<div class='project-git'>{''.join(status_badges)}</div>"
        f"<a href='{_escape(launch_session_url)}' class='btn-primary' style='text-decoration:none;font-size:0.85em;padding:6px 12px'>Launch Agent</a>"
        "</div>"
        "</div>"
        f"<div class='project-path' style='margin-top:4px'><code>{_escape(resolved)}</code></div>"
        "</section>"
    )

    remotes = detail.get("remotes") or []
    if remotes:
        remote_names: set[str] = set()
        remote_rows = ""
        for r in remotes:
            url = str(r.get("url", ""))
            rname = str(r.get("name", ""))
            if rname:
                remote_names.add(rname)
            remote_rows += (
                "<div class='remote-row'>"
                f"<span class='remote-name'>{_escape(rname)}</span>"
                f"<span class='remote-url'>{_escape(url)}</span>"
                f"<span class='remote-direction'>{_escape(str(r.get('direction', '')))}</span>"
                "</div>"
            )
        connectivity_html = (
            "<div id='remote_status' style='margin-top:8px'>"
            "<span class='ssh-status ssh-pending'>checking remote connectivity&hellip;</span>"
            "</div>"
        ) if remote_names else ""

        pull_push_forms = ""
        for rname in sorted(remote_names):
            pull_push_forms += (
                "<div class='git-action-row' style='display:flex;gap:6px;align-items:center;margin-top:8px'>"
                f"<span style='font-size:0.85em;min-width:60px'>{_escape(rname)}:</span>"
                f"<form method='post' action='/actions/project-git-pull' style='margin:0'>{path_hidden}<input type='hidden' name='remote' value='{_escape(rname)}'><button type='submit' class='btn-sm'>Pull</button></form>"
                f"<form method='post' action='/actions/project-git-push' style='margin:0'>{path_hidden}<input type='hidden' name='remote' value='{_escape(rname)}'><button type='submit' class='btn-sm'>Push</button></form>"
                "</div>"
            )

        remotes_html = (
            "<section class='panel detail-section'>"
            "<h3>Remotes</h3>"
            f"{remote_rows}"
            f"{connectivity_html}"
            f"{pull_push_forms}"
            "</section>"
        )
    else:
        remotes_html = "<section class='panel detail-section'><h3>Remotes</h3><div class='empty'>No remotes configured.</div></section>"

    status_output = str(detail.get("status_output", ""))
    diff_stat = str(detail.get("diff_stat", ""))
    if status_output:
        discard_form = (
            "<form method='post' action='/actions/project-git-discard' style='margin:0;display:inline' "
            "onsubmit=\"return confirm('This will discard ALL uncommitted changes. Continue?')\">"
            f"{path_hidden}"
            "<button type='submit' class='btn-sm btn-danger'>Discard All Changes</button>"
            "</form>"
        )
        commit_form = (
            "<div style='margin-top:12px'>"
            "<details id='diff_preview'>"
            "<summary style='cursor:pointer;font-size:0.85em;color:var(--muted)'>Show full diff</summary>"
            "<pre class='code-block' id='diff_content' style='max-height:400px;overflow:auto'>Loading&hellip;</pre>"
            "</details>"
            f"<form method='post' action='/actions/project-git-commit' style='margin-top:8px;display:flex;gap:6px;align-items:start'>{path_hidden}"
            "<input type='text' name='message' placeholder='Commit message' required style='flex:1;padding:6px 10px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text);font-size:0.9em'>"
            "<button type='submit' class='btn-primary' style='font-size:0.85em;padding:6px 14px;white-space:nowrap'>Commit All</button>"
            "</form></div>"
        )
        status_html = (
            "<section class='panel detail-section'>"
            "<div style='display:flex;align-items:center;justify-content:space-between'>"
            "<h3 style='margin:0'>Working Tree Status</h3>"
            f"{discard_form}"
            "</div>"
            f"<pre class='code-block'>{_escape(status_output)}</pre>"
            + (f"<pre class='code-block'>{_escape(diff_stat)}</pre>" if diff_stat else "")
            + commit_form
            + "</section>"
        )
    else:
        status_html = "<section class='panel detail-section'><h3>Working Tree Status</h3><div class='empty'>Clean working tree.</div></section>"

    branches = detail.get("branches") or []
    if branches:
        branch_items = ""
        for b in branches:
            is_current = str(b.get("current", "false")) == "true"
            bname = _escape(str(b.get("name", "")))
            if is_current:
                branch_items += f"<li class='branch-current'>* {bname}</li>"
            else:
                switch_form = (
                    f"<form method='post' action='/actions/project-git-switch' style='margin:0;display:inline'>{path_hidden}<input type='hidden' name='branch' value='{bname}'><button type='submit' class='btn-sm' style='margin-left:6px'>Switch</button></form>"
                )
                branch_items += f"<li>{bname} {switch_form}</li>"
        create_branch_form = (
            "<div style='margin-top:10px;display:flex;gap:6px'>"
            f"<form method='post' action='/actions/project-git-create-branch' style='margin:0;display:flex;gap:6px;flex:1'>{path_hidden}"
            "<input type='text' name='branch' placeholder='new-branch-name' required style='flex:1;padding:5px 8px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text);font-size:0.85em'>"
            "<button type='submit' class='btn-sm'>Create &amp; Switch</button>"
            "</form></div>"
        )
        branches_html = (
            "<section class='panel detail-section'>"
            f"<h3>Local Branches ({len(branches)})</h3>"
            f"<ul class='branch-list'>{branch_items}</ul>"
            f"{create_branch_form}"
            "</section>"
        )
    else:
        branches_html = "<section class='panel detail-section'><h3>Local Branches</h3><div class='empty'>No branches found.</div></section>"

    stash_list = detail.get("stash_list") or []
    stash_save_form = (
        "<div style='margin-top:8px;display:flex;gap:6px'>"
        f"<form method='post' action='/actions/project-git-stash' style='margin:0;display:flex;gap:6px;flex:1'>{path_hidden}"
        "<input type='text' name='stash_message' placeholder='Stash message (optional)' style='flex:1;padding:5px 8px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text);font-size:0.85em'>"
        "<button type='submit' class='btn-sm'>Stash</button>"
        "</form></div>"
    )
    if stash_list:
        pop_form = (
            f"<form method='post' action='/actions/project-git-stash-pop' style='margin:0;display:inline'>{path_hidden}<button type='submit' class='btn-sm' style='margin-left:6px'>Pop</button></form>"
        )
        stash_items = f"<li>{_escape(stash_list[0])} {pop_form}</li>"
        stash_items += "".join(f"<li>{_escape(s)}</li>" for s in stash_list[1:])
        stash_html = (
            "<section class='panel detail-section'>"
            f"<h3>Stash ({len(stash_list)})</h3>"
            f"<ul class='branch-list'>{stash_items}</ul>"
            f"{stash_save_form}"
            "</section>"
        )
    else:
        stash_html = (
            "<section class='panel detail-section'>"
            "<h3>Stash</h3>"
            "<div class='empty' style='margin-bottom:6px'>No stashed changes.</div>"
            f"{stash_save_form}"
            "</section>"
        )

    commits = detail.get("recent_commits") or []
    if commits:
        commit_rows = ""
        for c in commits:
            commit_rows += (
                "<tr>"
                f"<td class='commit-sha'>{_escape(str(c.get('sha', '')))}</td>"
                f"<td>{_escape(str(c.get('subject', '')))}</td>"
                f"<td>{_escape(str(c.get('author', '')))}</td>"
                f"<td style='white-space:nowrap'>{_escape(str(c.get('date', '')))}</td>"
                "</tr>"
            )
        commits_html = (
            "<section class='panel'>"
            f"<h3>Recent Commits ({len(commits)})</h3>"
            "<div class='table-scroll'>"
            "<table class='commit-table'>"
            "<thead><tr><th>sha</th><th>message</th><th>author</th><th>when</th></tr></thead>"
            f"<tbody>{commit_rows}</tbody>"
            "</table></div></section>"
        )
    else:
        commits_html = ""

    remote_names_for_detail: set[str] = set()
    for r in remotes:
        rname = str(r.get("name", ""))
        if rname:
            remote_names_for_detail.add(rname)
    remote_names_json = json.dumps(sorted(remote_names_for_detail))
    project_path_json = json.dumps(resolved)

    detail_script = f"""\
window.addEventListener('DOMContentLoaded', async () => {{
  const remoteNames = {remote_names_json};
  const projectPath = {project_path_json};
  const container = document.getElementById('remote_status');
  if (container && remoteNames.length > 0) {{
    const results = [];
    for (const name of remoteNames) {{
      try {{
        const params = new URLSearchParams({{ path: projectPath, remote: name }});
        const resp = await fetch('/api/project-remote-check?' + params.toString());
        const data = await resp.json();
        results.push(data);
      }} catch (e) {{
        results.push({{ remote: name, ok: false, message: 'fetch error' }});
      }}
    }}
    let html = '';
    for (const r of results) {{
      const cls = r.ok ? 'ssh-ok' : 'ssh-fail';
      const label = r.ok ? 'connected' : 'unreachable';
      const proto = r.protocol ? ' (' + esc(r.protocol) + ')' : '';
      const remote = r.remote ? esc(r.remote) : '';
      const msg = (!r.ok && r.message && r.message !== 'connected') ? ' &mdash; ' + esc(r.message) : '';
      html += `<div class="ssh-status ${{cls}}">${{remote}}${{proto}}: ${{label}}${{msg}}</div>`;
      if (r.hint) {{
        html += `<div class="help" style="margin-top:4px;font-size:0.85em">${{esc(r.hint)}}</div>`;
      }}
    }}
    container.innerHTML = html;
  }}
  const diffToggle = document.getElementById('diff_preview');
  if (diffToggle) {{
    let loaded = false;
    diffToggle.addEventListener('toggle', async () => {{
      if (diffToggle.open && !loaded) {{
        loaded = true;
        try {{
          const params = new URLSearchParams({{ path: projectPath }});
          const resp = await fetch('/api/project-git-diff?' + params.toString());
          const data = await resp.json();
          document.getElementById('diff_content').textContent = data.diff || '(no diff)';
        }} catch (e) {{
          document.getElementById('diff_content').textContent = 'Failed to load diff.';
        }}
      }}
    }});
  }}
}});
function esc(s) {{
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}}
"""

    body = (
        "<main class='single-column'><div class='column'>"
        f"{header_html}"
        "<div class='detail-grid'>"
        f"{remotes_html}"
        f"{branches_html}"
        f"{stash_html}"
        "</div>"
        f"{status_html}"
        f"{commits_html}"
        "</div></main>"
    )
    return app._page_shell(active_nav="Projects", body=body, extra_script=detail_script)
