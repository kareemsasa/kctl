from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .types import PlanError
from .ui_dashboard_support import (
    _detect_token_warning,
    _escape,
    _page_link,
    _render_collapsible_section,
    _status_class,
    available_providers,
)


def _tail_lines_text(text: str | None, line_count: int = 50) -> str:
    if not text:
        return ""
    return "\n".join(text.splitlines()[-line_count:])


def _session_detail_link(session_id: str) -> str:
    return _page_link(f"/sessions/{session_id}")


def _format_session_ts(value: object) -> str:
    text = str(value or "")
    if text and "T" in text:
        return text.replace("T", " ").split(".")[0] + " UTC"
    return text


def _session_status_counts(sessions: list[dict[str, object]]) -> tuple[int, int, int]:
    running = sum(1 for s in sessions if str(s.get("status") or "") == "running")
    completed = sum(1 for s in sessions if str(s.get("status") or "") == "completed")
    failed = sum(1 for s in sessions if str(s.get("status") or "") == "failed")
    return running, completed, failed


def sessions_dir(repo_path: Path) -> Path:
    return repo_path / ".kctl" / "sessions"


def session_dir(repo_path: Path, session_id: str) -> Path:
    return sessions_dir(repo_path) / session_id


def session_meta_path(repo_path: Path, session_id: str) -> Path:
    return session_dir(repo_path, session_id) / "meta.json"


def session_output_path(repo_path: Path, session_id: str) -> Path:
    return session_dir(repo_path, session_id) / "output.log"


def write_session_meta(repo_path: Path, meta: dict[str, object]) -> None:
    path = session_meta_path(repo_path, str(meta["id"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(meta, indent=2) + "\n")


def read_session_meta(repo_path: Path, session_id: str) -> dict[str, object] | None:
    path = session_meta_path(repo_path, session_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def list_sessions(repo_path: Path) -> list[dict[str, object]]:
    root = sessions_dir(repo_path)
    if not root.exists():
        return []
    sessions: list[dict[str, object]] = []
    for child in sorted(root.iterdir(), reverse=True):
        if not child.is_dir():
            continue
        meta = read_session_meta(repo_path, child.name)
        if meta is not None:
            sessions.append(meta)
    sessions.sort(key=lambda s: str(s.get("started_at") or ""), reverse=True)
    return sessions


def get_session(repo_path: Path, session_id: str) -> dict[str, object] | None:
    return read_session_meta(repo_path, session_id)


def read_session_output(repo_path: Path, session_id: str) -> str:
    path = session_output_path(repo_path, session_id)
    if not path.exists():
        return ""
    try:
        return path.read_text()
    except OSError:
        return ""


def run_session_subprocess(
    repo_path: Path,
    meta: dict[str, object],
    command: list[str],
    output_path: Path,
) -> None:
    resolved = str(meta["project_path"])
    session_id = str(meta["id"])

    def _run() -> None:
        final_status = "failed"
        final_exit_code: int | None = -1
        try:
            process = subprocess.Popen(
                command,
                cwd=resolved,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            meta["pid"] = process.pid
            write_session_meta(repo_path, meta)

            with output_path.open("a", encoding="utf-8") as log:
                assert process.stdout is not None
                for line in iter(process.stdout.readline, ""):
                    log.write(line)
                    log.flush()
                process.stdout.close()

            exit_code = process.wait()
            final_status = "completed" if exit_code == 0 else "failed"
            final_exit_code = exit_code
        except Exception as exc:
            with output_path.open("a", encoding="utf-8") as log:
                log.write(f"\n[kctl] session error: {exc}\n")
        finally:
            fresh = read_session_meta(repo_path, session_id) or meta
            fresh["status"] = final_status
            fresh["exit_code"] = final_exit_code
            fresh["ended_at"] = datetime.now(timezone.utc).isoformat()
            fresh["pid"] = None
            fresh["token_warning"] = _detect_token_warning(output_path)
            write_session_meta(repo_path, fresh)

    threading.Thread(target=_run, daemon=True).start()


def start_agent_session(repo_path: Path, project_path: str, prompt: str, provider: str) -> str:
    resolved = str(Path(project_path).expanduser().resolve())
    if not Path(resolved).is_dir():
        raise PlanError(f"Project path is not a directory: {resolved}")

    session_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    provider_session_id = str(uuid.uuid4())
    output_path = session_output_path(repo_path, session_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.touch()

    now = datetime.now(timezone.utc).isoformat()
    meta: dict[str, object] = {
        "id": session_id,
        "project_path": resolved,
        "project_name": Path(resolved).name,
        "prompt": prompt,
        "provider": provider,
        "provider_session_id": provider_session_id,
        "status": "running",
        "started_at": now,
        "ended_at": None,
        "exit_code": None,
        "pid": None,
        "messages": [{"role": "user", "content": prompt, "timestamp": now}],
    }
    write_session_meta(repo_path, meta)

    if provider == "claude":
        command = [
            "claude", "--dangerously-skip-permissions",
            "--session-id", provider_session_id,
            "-p", prompt,
        ]
    else:
        command = ["codex", "exec", "--full-auto", "--cd", resolved, prompt]

    run_session_subprocess(repo_path, meta, command, output_path)
    return session_id


def reply_to_session(repo_path: Path, session_id: str, reply: str) -> None:
    meta = read_session_meta(repo_path, session_id)
    if not meta:
        raise PlanError(f"Session not found: {session_id}")
    if meta.get("status") == "running":
        raise PlanError("Session is still running. Wait for it to finish before replying.")

    provider = str(meta.get("provider") or "codex")
    provider_session_id = str(meta.get("provider_session_id") or "")
    output_path = session_output_path(repo_path, session_id)

    now = datetime.now(timezone.utc).isoformat()
    messages = list(meta.get("messages") or [])
    messages.append({"role": "user", "content": reply, "timestamp": now})
    meta["messages"] = messages
    meta["status"] = "running"
    meta["ended_at"] = None
    meta["exit_code"] = None
    write_session_meta(repo_path, meta)

    with output_path.open("a", encoding="utf-8") as log:
        log.write(f"\n{'─' * 60}\n")
        log.write(f"[follow-up #{len(messages)}]\n")
        log.write(f"{'─' * 60}\n\n")

    if provider == "claude" and provider_session_id:
        command = [
            "claude", "--dangerously-skip-permissions",
            "--resume", provider_session_id,
            "-p", reply,
        ]
    else:
        command = ["codex", "exec", "resume", "--last", "--full-auto", reply]

    run_session_subprocess(repo_path, meta, command, output_path)


def stop_agent_session(repo_path: Path, session_id: str) -> bool:
    meta = read_session_meta(repo_path, session_id)
    if not meta:
        return False
    pid = meta.get("pid")
    if not pid or meta.get("status") != "running":
        return False
    try:
        os.kill(int(pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        pass
    return True


def render_sessions_page(app: object, *, action_message: str | None = None, prefill_project: str | None = None) -> str:
    providers = available_providers()
    tracked_projects = app.load_tracked_projects()
    sessions = list_sessions(app.repo_path)

    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
    running_count, completed_count, failed_count = _session_status_counts(sessions)

    project_options = "".join(
        f"<option value='{_escape(p)}'{' selected' if prefill_project and str(Path(prefill_project).expanduser().resolve()) == p else ''}>"
        f"{_escape(Path(p).name)} &mdash; {_escape(p)}</option>"
        for p in tracked_projects
    )
    if not tracked_projects:
        project_select = "<div class='help'>No tracked projects. <a href='/projects'>Add a project</a> first.</div>"
    else:
        project_select = (
            f"<label for='session_project'><strong>Project</strong></label>"
            f"<select id='session_project' name='project_path' required>{project_options}</select>"
        )

    if providers:
        provider_options = "".join(
            f"<option value='{_escape(v)}'>{_escape(l)}</option>"
            for v, l in providers
        )
        provider_select = (
            f"<label for='session_provider'><strong>Provider</strong></label>"
            f"<select id='session_provider' name='provider'>{provider_options}</select>"
        )
    else:
        provider_select = "<div class='help'>No agent providers found. Install <code>codex</code> or <code>claude</code>.</div>"

    session_list_html = ""
    if sessions:
        for s in sessions:
            sid = str(s.get("id") or "")
            status = str(s.get("status") or "unknown")
            provider_name = str(s.get("provider") or "")
            project_name = str(s.get("project_name") or Path(str(s.get("project_path") or "")).name)
            prompt_preview = str(s.get("prompt") or "")
            if len(prompt_preview) > 120:
                prompt_preview = prompt_preview[:120] + "..."
            started = _format_session_ts(s.get("started_at"))
            badge_cls = f"session-badge-{status}" if status in {"running", "completed", "failed"} else "session-badge-provider"
            detail_url = _session_detail_link(sid)
            turn_count = len(s.get("messages") or []) if isinstance(s.get("messages"), list) else 0
            session_list_html += (
                f"<a class='session-item {_status_class(status)}' href='{detail_url}'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap'>"
                f"<strong>{_escape(project_name)}</strong>"
                f"<div style='display:flex;gap:6px'>"
                f"<span class='session-badge session-badge-provider'>{_escape(provider_name)}</span>"
                f"<span class='session-badge {_escape(badge_cls)}'>{_escape(status)}</span>"
                f"</div></div>"
                f"<div class='session-prompt-preview'>{_escape(prompt_preview)}</div>"
                f"<div class='session-meta' style='display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap'>"
                f"<span>{_escape(started)}</span>"
                f"<span>{_escape(turn_count)} turn{'s' if turn_count != 1 else ''}</span>"
                "</div>"
                f"</a>"
            )
    else:
        session_list_html = "<div class='empty'>No sessions yet. Launch one above.</div>"

    overview_html = (
        "<div class='overview-bar'>"
        f"<span><strong>{_escape(len(sessions))}</strong> sessions</span>"
        f"<span><strong>{_escape(running_count)}</strong> running</span>"
        f"<span><strong>{_escape(completed_count)}</strong> completed</span>"
        f"<span class='{_status_class('failure') if failed_count else ''}'><strong>{_escape(failed_count)}</strong> failed</span>"
        "</div>"
    )
    launch_section = _render_collapsible_section(
        "Launch Session",
        (
            "<div class='help'>Run an ad-hoc prompt against a project using Claude or Codex. No YAML plan required.</div>"
            "<form method='post' action='/actions/start-session'>"
            f"{project_select}"
            f"{provider_select}"
            "<label for='session_prompt'><strong>Prompt</strong></label>"
            "<textarea id='session_prompt' name='prompt' rows='6' placeholder='Describe what you want the agent to do...' required></textarea>"
            "<button type='submit' class='btn-primary'>Launch Session</button>"
            "</form>"
        ),
        open_by_default=not sessions,
    )
    body = (
        f"{overview_html}"
        "<main class='single-column'><div class='column'>"
        f"{notice_html}"
        f"{launch_section}"
        "<section class='panel'>"
        "<h2>Recent Sessions</h2>"
        f"{session_list_html}"
        "</section>"
        "</div></main>"
    )
    return app._page_shell(active_nav="Sessions", body=body)


def render_session_detail_page(app: object, session_id: str) -> str:
    meta = get_session(app.repo_path, session_id)
    if not meta:
        return app._page_shell(
            active_nav="Sessions",
            body=(
                "<main class='single-column'><div class='column'>"
                "<a class='back-link' href='/sessions'>&larr; All Sessions</a>"
                f"<section class='panel'><div class='empty'>Session not found: {_escape(session_id)}</div></section>"
                "</div></main>"
            ),
        )

    status = str(meta.get("status") or "unknown")
    provider_name = str(meta.get("provider") or "")
    project_path = str(meta.get("project_path") or "")
    project_name = str(meta.get("project_name") or Path(project_path).name)
    messages = meta.get("messages") or []
    prompt = str(meta.get("prompt") or "")
    started = str(meta.get("started_at") or "")
    ended = meta.get("ended_at")
    exit_code = meta.get("exit_code")
    token_warning = meta.get("token_warning")
    output = read_session_output(app.repo_path, session_id)

    started_display = _format_session_ts(started)
    ended_display = ""
    if ended:
        ended_display = _format_session_ts(ended)

    badge_cls = f"session-badge-{status}" if status in {"running", "completed", "failed"} else "session-badge-provider"
    message_count = len(messages) if isinstance(messages, list) else 0
    turn_label = f"{message_count} turn{'s' if message_count != 1 else ''}"

    info_html = (
        "<div style='display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px'>"
        f"<span class='session-badge session-badge-provider'>{_escape(provider_name)}</span>"
        f"<span class='session-badge {_escape(badge_cls)}' id='status_badge'>{_escape(status)}</span>"
        f"<span class='session-badge session-badge-provider' id='turn_badge'>{_escape(turn_label)}</span>"
        "</div>"
        f"<div><strong>Project:</strong> {_escape(project_name)} &mdash; <code>{_escape(project_path)}</code></div>"
        f"<div><strong>Started:</strong> {_escape(started_display)}</div>"
    )
    if ended_display:
        info_html += f"<div><strong>Ended:</strong> {_escape(ended_display)}</div>"
    if exit_code is not None:
        info_html += f"<div><strong>Exit code:</strong> {_escape(str(exit_code))}</div>"

    token_warning_html = ""
    if token_warning:
        token_warning_html = (
            "<div id='token_warning' class='notice' style='background:#fef3cd;color:#856404;border:1px solid #ffc107;"
            "border-radius:6px;padding:10px 14px;margin-top:10px;font-size:0.9em'>"
            f"<strong>Token/quota warning:</strong> {_escape(str(token_warning))}"
            "</div>"
        )

    stop_html = ""
    if status == "running":
        stop_html = (
            "<form method='post' action='/actions/stop-session' style='margin:0;display:inline'>"
            f"<input type='hidden' name='session_id' value='{_escape(session_id)}'>"
            "<button type='submit' class='btn-danger'>Stop Session</button>"
            "</form>"
        )

    conversation_inner = ""
    if isinstance(messages, list) and messages:
        for i, msg in enumerate(messages):
            content = str(msg.get("content") or "") if isinstance(msg, dict) else str(msg)
            ts = ""
            if isinstance(msg, dict) and msg.get("timestamp"):
                ts_raw = str(msg["timestamp"])
                ts = ts_raw.replace("T", " ").split(".")[0] + " UTC" if "T" in ts_raw else ts_raw
            num = i + 1
            conversation_inner += (
                "<div class='session-message'>"
                "<div class='session-message-header'>"
                f"<strong>Prompt #{num}</strong>"
                f"{f'<span class=\"session-meta\">{_escape(ts)}</span>' if ts else ''}"
                "</div>"
                f"<div class='session-prompt-full'>{_escape(content)}</div>"
                "</div>"
            )
    else:
        conversation_inner = f"<div class='session-prompt-full'>{_escape(prompt)}</div>"
    conversation_html = f"<div id='conversation_list'>{conversation_inner}</div>"

    can_reply = status in {"completed", "failed"}
    reply_html = ""
    provider_label = provider_name.capitalize() if provider_name else "Agent"
    if can_reply:
        reply_html = (
            "<section class='panel' id='reply_section'>"
            "<h2>Reply</h2>"
            "<form method='post' action='/actions/session-reply'>"
            f"<input type='hidden' name='session_id' value='{_escape(session_id)}'>"
            "<textarea name='reply' rows='4' placeholder='Send a follow-up message...' required style='min-height:80px' id='reply_input'></textarea>"
            f"<button type='submit' class='btn-primary'>Send Reply via {_escape(provider_label)}</button>"
            "</form>"
            "</section>"
        )

    tail_output = _tail_lines_text(output)
    output_html = (
        "<div style='display:flex;justify-content:space-between;align-items:center;gap:8px'>"
        "<h2>Output</h2>"
        "<button type='button' class='mini-button' data-copy-target='#session_output' data-copy-last-lines='50' onclick='return window.kctlCopyButtonClick(this)'>Copy last 50 lines</button>"
        "</div>"
        "<div class='help'>Mobile fallback: tap the tail box below to select text with the browser copy UI.</div>"
        f"<textarea id='session_output_tail' class='code-block' readonly onclick='this.focus();this.select();' style='min-height:120px'>{_escape(tail_output)}</textarea>"
        f"<pre class='session-output' id='session_output'>{_escape(output) if output else '(waiting for output...)'}</pre>"
    )
    conversation_section = _render_collapsible_section(
        "Conversation",
        conversation_html,
        open_by_default=status != "running",
    )

    body = (
        "<main class='single-column'><div class='column'>"
        "<a class='back-link' href='/sessions'>&larr; All Sessions</a>"
        "<section class='panel'>"
        "<div style='display:flex;justify-content:space-between;align-items:flex-start;gap:12px'>"
        f"<h2 style='margin:0'>Session: {_escape(project_name)}</h2>"
        f"{stop_html}"
        "</div>"
        f"{info_html}"
        f"{token_warning_html}"
        "</section>"
        "<section class='panel'>"
        f"{output_html}"
        "</section>"
        f"{conversation_section}"
        f"{reply_html}"
        "</div></main>"
    )

    session_id_json = json.dumps(session_id)
    status_json = json.dumps(status)
    provider_label_json = json.dumps(provider_label)
    detail_script = (
        "window.addEventListener('DOMContentLoaded', () => {\n"
        f"  const sessionId = {session_id_json};\n"
        f"  const sessionStatus = {status_json};\n"
        f"  const providerLabel = {provider_label_json};\n"
        "  const outputNode = document.getElementById('session_output');\n"
        "  if (!sessionId || !outputNode || sessionStatus !== 'running') return;\n"
        "  let knownMsgCount = document.querySelectorAll('.session-message').length;\n"
        "  function escapeHtml(s) {\n"
        "    const d = document.createElement('div'); d.textContent = s; return d.innerHTML;\n"
        "  }\n"
        "  function renderMessages(msgs) {\n"
        "    const conv = document.getElementById('conversation_list');\n"
        "    if (!conv || !Array.isArray(msgs) || msgs.length <= knownMsgCount) return;\n"
        "    knownMsgCount = msgs.length;\n"
        "    let html = '';\n"
        "    msgs.forEach((m, i) => {\n"
        "      const content = (typeof m === 'object' && m.content) ? m.content : String(m);\n"
        "      let ts = '';\n"
        "      if (typeof m === 'object' && m.timestamp) {\n"
        "        ts = m.timestamp.replace('T', ' ').split('.')[0] + ' UTC';\n"
        "      }\n"
        "      html += '<div class=\"session-message\">'\n"
        "        + '<div class=\"session-message-header\">'\n"
        "        + '<strong>Prompt #' + (i + 1) + '</strong>'\n"
        "        + (ts ? '<span class=\"session-meta\">' + escapeHtml(ts) + '</span>' : '')\n"
        "        + '</div>'\n"
        "        + '<div class=\"session-prompt-full\">' + escapeHtml(content) + '</div>'\n"
        "        + '</div>';\n"
        "    });\n"
        "    conv.innerHTML = html;\n"
        "    const turnBadge = document.getElementById('turn_badge');\n"
        "    if (turnBadge) {\n"
        "      const n = msgs.length;\n"
        "      turnBadge.textContent = n + ' turn' + (n !== 1 ? 's' : '');\n"
        "    }\n"
        "  }\n"
        "  const refreshOutput = async () => {\n"
        "    const params = new URLSearchParams({ id: sessionId });\n"
        "    const response = await fetch(`/api/session-output?${params.toString()}`);\n"
        "    if (!response.ok) return;\n"
        "    const data = await response.json();\n"
        "    outputNode.textContent = data.output || '(waiting for output...)';\n"
        "    const tailNode = document.getElementById('session_output_tail');\n"
        "    if (tailNode) {\n"
        "      const lines = (data.output || '').split(/\\r?\\n/);\n"
        "      tailNode.value = lines.slice(-50).join('\\n');\n"
        "    }\n"
        "    outputNode.scrollTop = outputNode.scrollHeight;\n"
        "    if (data.messages) renderMessages(data.messages);\n"
        "    if (data.status !== 'running') {\n"
        "      clearInterval(window._sessionPoll);\n"
        "      const badge = document.getElementById('status_badge');\n"
        "      if (badge) {\n"
        "        badge.textContent = data.status;\n"
        "        badge.className = 'session-badge session-badge-' + data.status;\n"
        "      }\n"
        "      if (data.token_warning && !document.getElementById('token_warning')) {\n"
        "        const panel = document.querySelector('.panel');\n"
        "        if (panel) {\n"
        "          const warn = document.createElement('div');\n"
        "          warn.id = 'token_warning';\n"
        "          warn.className = 'notice';\n"
        "          warn.style.cssText = 'background:#fef3cd;color:#856404;border:1px solid #ffc107;border-radius:6px;padding:10px 14px;margin-top:10px;font-size:0.9em';\n"
        "          warn.innerHTML = '<strong>Token/quota warning:</strong> ' + escapeHtml(data.token_warning);\n"
        "          panel.appendChild(warn);\n"
        "        }\n"
        "      }\n"
        "      const replySection = document.getElementById('reply_section');\n"
        "      if (!replySection) {\n"
        "        const col = document.querySelector('.column');\n"
        "        if (col) {\n"
        "          const section = document.createElement('section');\n"
        "          section.id = 'reply_section';\n"
        "          section.className = 'panel';\n"
        "          section.innerHTML = '<h2>Reply</h2>'\n"
        "           + '<form method=\"post\" action=\"/actions/session-reply\">'\n"
        "           + '<input type=\"hidden\" name=\"session_id\" value=\"' + sessionId + '\">'\n"
        "           + '<textarea name=\"reply\" rows=\"4\" placeholder=\"Send a follow-up message...\" required '\n"
        "           + 'style=\"min-height:80px\" id=\"reply_input\"></textarea>'\n"
        "           + '<button type=\"submit\" class=\"btn-primary\">Send Reply via ' + escapeHtml(providerLabel) + '</button>'\n"
        "           + '</form>';\n"
        "          col.appendChild(section);\n"
        "          const input = document.getElementById('reply_input');\n"
        "          if (input) input.focus();\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "  };\n"
        "  refreshOutput();\n"
        "  window._sessionPoll = setInterval(refreshOutput, 1500);\n"
        "});\n"
    )
    return app._page_shell(active_nav="Sessions", body=body, extra_script=detail_script)
