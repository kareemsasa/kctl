from __future__ import annotations

from typing import Callable

from .plan import load_plan_templates
from .paths import project_root
from .ui_dashboard_support import (
    _escape,
    _preflight_status_tone,
    _provider_select_html,
    _render_preflight_item_html,
    _render_selection_list,
    available_providers,
    list_plans_in_directory,
)


def render_actions_page(
    app: object,
    *,
    action_message: str | None = None,
    summarize_preflight: Callable[..., dict[str, object]],
) -> str:
    templates = load_plan_templates(project_root())
    plan_templates = [
        (template_name, template.get("description") if isinstance(template, dict) else None)
        for template_name, template in templates.items()
    ]
    tracked_projects = app.load_tracked_projects()
    providers = available_providers()
    plans_status, plans_message, initial_plans = list_plans_in_directory(str(app.default_plans_dir))
    launch_preflight = summarize_preflight(
        str(app.repo_path),
        str(app.default_plans_dir),
        selected_plan_names=None,
        provider_override=None,
    )
    preflight_items = launch_preflight.get("items", {})
    preflight_html = "".join(
        _render_preflight_item_html(label, item)
        for label, item in (
            ("Repo", preflight_items.get("repo") or {}),
            ("Plans Dir", preflight_items.get("plans_dir") or {}),
            ("Binaries", preflight_items.get("binaries") or {}),
            ("Writable Paths", preflight_items.get("writable_paths") or {}),
            ("Required Env", preflight_items.get("required_env") or {}),
        )
    )
    tracked_projects_html = _render_selection_list(
        "project_paths",
        [(project_path, project_path) for project_path in tracked_projects],
        empty_html="<div class='help'>No tracked projects yet. <a href='/projects'>Manage projects</a></div>",
    )
    initial_plans_html = _render_selection_list(
        "selected_plans",
        [(plan, plan) for plan in initial_plans],
        heading="Plans found",
        item_class="",
    )
    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
    body = (
        f"<main class='single-column'>"
        f"<div class='column'>"
        f"{notice_html}"
        f"<section class='panel'>"
        f"<h2>Refresh Index</h2>"
        f"<div class='help'>Refreshes the dashboard data from saved runs and workspaces on this machine.</div>"
        f"<form method='post' action='/actions/index'>"
        f"<button type='submit'>Refresh Index</button>"
        f"</form>"
        f"</section>"
        f"<section class='panel'>"
        f"<h2>Run Plans</h2>"
        f"<div class='help'>Runs every plan in the plans folder for the target project.</div>"
        f"<form method='post' action='/actions/run-many'>"
        f"<label for='target_repo_run_many'><strong>Target Repo</strong></label>"
        f"<input id='target_repo_run_many' name='target_repo' type='text' value='{_escape(app.repo_path)}' required>"
        f"<div id='target_repo_run_many_status' class='repo-check'></div>"
        f"<div><strong>Plans Dir</strong>: <code>{_escape(app.default_plans_dir)}</code></div>"
        f"<label for='plans_dir'><strong>Plans Dir Override</strong></label>"
        f"<input id='plans_dir' name='plans_dir' type='text' placeholder='Optional override'>"
        f"<div id='plans_dir_status' class='repo-check' data-status='{_escape(plans_status)}'>{_escape(plans_message)}</div>"
        f"<div id='plans_dir_preview' class='plans-preview'>{initial_plans_html}</div>"
        f"<div><strong>Tracked Projects</strong></div>"
        f"{tracked_projects_html}"
        f"<div class='preflight-summary'>"
        f"<div class='launch-decision launch-decision-{_escape(_preflight_status_tone(str(launch_preflight.get('status') or 'warn')))}' id='run_many_launch_decision'>{_escape(launch_preflight.get('decision') or 'Runnable with warnings')}</div>"
        f"<div><strong>Launch Preflight</strong></div>"
        f"<div id='run_many_preflight_message' class='repo-check' data-status='{_escape(launch_preflight.get('status'))}'>{_escape(launch_preflight.get('message'))}</div>"
        f"<div id='run_many_preflight' class='preflight-grid'>{preflight_html}</div>"
        f"</div>"
        f"<label for='concurrency'><strong>Concurrency</strong></label>"
        f"<input id='concurrency' name='concurrency' type='number' min='1' value='1'>"
        f"<div class='help'>How many plans can run at the same time. Use 1 for the safest option.</div>"
        + (_provider_select_html("provider_override", providers) if providers else "")
        + "<button type='submit' id='run_many_submit_button'>Run Plans</button>"
        + "</form>"
        + "</section>"
        + "<section class='panel'>"
        + "<h2>Run Plan Across Projects</h2>"
        + "<div class='help'>Run a single plan file against every tracked project. "
        + "Select one plan from the list above, then choose which projects to target.</div>"
        + f"<form method='post' action='/actions/run-plan-across-projects'>"
        + f"<input type='hidden' name='target_repo' value='{_escape(app.repo_path)}'>"
        + f"<input type='hidden' name='plans_dir' value=''>"
        + "<label for='cross_selected_plans'><strong>Plan</strong></label>"
        + "<div class='help'>Use the Plans Dir listing above to identify the plan filename, then enter it here.</div>"
        + "<input id='cross_selected_plans' name='selected_plans' type='text' placeholder='001-sample.yaml' required>"
        + "<label><strong>Target Projects</strong></label>"
        + (tracked_projects_html if tracked_projects else "<div class='help'>No tracked projects. <a href='/projects'>Add projects</a> first.</div>")
        + (_provider_select_html("provider_override", providers) if providers else "")
        + "<button type='submit' id='run_single_across_projects_button'>Run Across Projects</button>"
        + "</form>"
        + "</section>"
        + "<section class='panel'>"
        + "<h2>Create Plan</h2>"
        + "<div class='help'>Creates one new plan file in the target project's plans folder.</div>"
        + "<form method='post' action='/actions/create-plan'>"
        + f"<label for='target_repo_create_plan'><strong>Target Repo</strong></label>"
        + f"<input id='target_repo_create_plan' name='target_repo' type='text' value='{_escape(app.repo_path)}' required>"
        + "<div id='target_repo_create_plan_status' class='repo-check'></div>"
        + "<label for='template_name'><strong>Template</strong></label>"
        + "<select id='template_name' name='template_name'>"
        + "".join(
            f"<option value='{_escape(name)}'>{_escape(name)}"
            + (f" - {_escape(desc)}" if desc else "")
            + "</option>"
            for name, desc in plan_templates
        )
        + "</select>"
        + f"<div><strong>Plan Root</strong>: <code>{_escape(app.default_plans_dir)}</code></div>"
        + "<label for='output_path'><strong>Plan File Name</strong></label>"
        + "<input id='output_path' name='output_path' type='text' placeholder='001-sample.yaml' required>"
        + "<label for='objective'><strong>Objective</strong></label>"
        + "<textarea id='objective' name='objective' rows='5' placeholder='Describe the change' required></textarea>"
        + "<label class='checkbox'><input name='force' type='checkbox' value='1'> Overwrite if the file exists</label>"
        + "<button type='submit'>Create Plan</button>"
        + "</form>"
        + "</section>"
        + "</div>"
        + "</main>"
    )
    actions_script = """\
function renderPreflight(preflight, messageId, containerId) {
  const message = document.getElementById(messageId);
  const container = document.getElementById(containerId);
  const decision = document.getElementById('run_many_launch_decision');
  if (!message || !container || !preflight) return;
  message.dataset.status = preflight.status || 'unknown';
  message.textContent = preflight.message || '';
  const bannerTone = preflight.status === 'pass' || preflight.status === 'ok'
    ? 'pass'
    : (preflight.status === 'block' || preflight.status === 'blocked' || preflight.status === 'error')
      ? 'block'
      : 'warn';
  if (decision) {
    decision.className = `launch-decision launch-decision-${bannerTone}`;
    decision.textContent = preflight.decision || (bannerTone === 'pass' ? 'Ready to run' : bannerTone === 'block' ? 'Blocked' : 'Runnable with warnings');
  }
  const labels = [
    ['repo', 'Repo'],
    ['plans_dir', 'Plans Dir'],
    ['binaries', 'Binaries'],
    ['writable_paths', 'Writable Paths'],
    ['required_env', 'Required Env'],
  ];
  container.innerHTML = labels.map(([key, label]) => {
    const item = (preflight.items || {})[key] || {};
    const details = item.details ? `<div class="help">${item.details}</div>` : '';
    const remediation = item.remediation ? `<div class="help"><strong>Fix:</strong> ${item.remediation}</div>` : '';
    const action = item.action_label && item.action_value
      ? `<button type="button" class="mini-button" data-copy="${item.action_value}">${item.action_label}</button>`
      : '';
    const tone = item.status === 'pass' || item.status === 'ok'
      ? 'pass'
      : (item.status === 'block' || item.status === 'blocked' || item.status === 'error' || item.status === 'missing' || item.status === 'not_dir' || item.status === 'empty')
        ? 'block'
        : 'warn';
    const statusClass = tone === 'pass' ? 'status-success' : tone === 'block' ? 'status-failure' : 'status-neutral';
    return `<div class="preflight-item ${statusClass}"><div><strong>${label}</strong> <span class="preflight-badge preflight-badge-${tone}">${tone.toUpperCase()}</span></div><div>${item.summary || ''}</div>${details}${remediation}${action}</div>`;
  }).join('');
  wireCopyButtons(container);
}
function wirePlansPreview(targetRepoInputId, plansDirInputId, statusId, previewId, preflightMessageId, preflightContainerId) {
  const targetRepoInput = document.getElementById(targetRepoInputId);
  const plansDirInput = document.getElementById(plansDirInputId);
  const runManyForm = plansDirInput ? plansDirInput.closest('form') : null;
  const providerOverrideInput = runManyForm ? runManyForm.querySelector('select[name="provider_override"]') : null;
  const runManyButton = document.getElementById('run_many_submit_button');
  const runAcrossProjectsButton = document.getElementById('run_single_across_projects_button');
  const status = document.getElementById(statusId);
  const preview = document.getElementById(previewId);
  if (!targetRepoInput || !plansDirInput || !status || !preview) return;
  let timer = null;
  function updateRunButtonLabels() {
    const selectedCount = preview.querySelectorAll('input[name="selected_plans"]:checked').length;
    if (runManyButton) {
      runManyButton.textContent = selectedCount === 1 ? 'Run Plan' : 'Run Plans';
    }
    if (runAcrossProjectsButton) {
      runAcrossProjectsButton.disabled = selectedCount !== 1;
      runAcrossProjectsButton.textContent = selectedCount === 1
        ? 'Run Plan Across Projects'
        : 'Select One Plan to Run Across Projects';
    }
  }
  function resolvedPlansDir() {
    const overrideValue = plansDirInput.value.trim();
    if (overrideValue) return overrideValue;
    const repoValue = targetRepoInput.value.trim();
    if (!repoValue) return "";
    return repoValue.replace(/\\/+$/, "") + "/.kctl/plans";
  }
  async function refreshPreflight() {
    const selectedPlans = Array.from(preview.querySelectorAll('input[name="selected_plans"]:checked')).map((node) => node.value);
    const params = new URLSearchParams({
      target_repo: targetRepoInput.value.trim(),
      plans_dir: resolvedPlansDir(),
    });
    if (providerOverrideInput && providerOverrideInput.value.trim()) {
      params.set('provider_override', providerOverrideInput.value.trim());
    }
    selectedPlans.forEach((plan) => params.append('selected_plans', plan));
    const response = await fetch(`/api/preflight?${params.toString()}`);
    const data = await response.json();
    renderPreflight(data, preflightMessageId, preflightContainerId);
  }
  async function refreshPreview() {
    const params = new URLSearchParams({ path: resolvedPlansDir() });
    const response = await fetch(`/api/list-plans?${params.toString()}`);
    const data = await response.json();
    status.dataset.status = data.status;
    status.textContent = data.message;
    if (!data.plans || data.plans.length === 0) {
      preview.innerHTML = "";
      updateRunButtonLabels();
      refreshPreflight();
      return;
    }
    preview.innerHTML =
      "<strong>Plans found</strong>" +
      data.plans.map((plan) => `<label><input type="checkbox" name="selected_plans" value="${plan}"> <span>${plan}</span></label>`).join("");
    preview.querySelectorAll('input[name="selected_plans"]').forEach((node) => {
      node.addEventListener('change', () => {
        updateRunButtonLabels();
        refreshPreflight();
      });
    });
    updateRunButtonLabels();
    refreshPreflight();
  }
  function scheduleRefresh() {
    clearTimeout(timer);
    timer = setTimeout(refreshPreview, 150);
  }
  targetRepoInput.addEventListener('input', scheduleRefresh);
  plansDirInput.addEventListener('input', scheduleRefresh);
  if (providerOverrideInput) {
    providerOverrideInput.addEventListener('change', scheduleRefresh);
  }
  refreshPreview();
}
window.addEventListener('DOMContentLoaded', () => {
  wireCopyButtons(document);
  wireRepoCheck('target_repo_run_many', 'target_repo_run_many_status');
  wireRepoCheck('target_repo_create_plan', 'target_repo_create_plan_status');
  wirePlansPreview(
    'target_repo_run_many',
    'plans_dir',
    'plans_dir_status',
    'plans_dir_preview',
    'run_many_preflight_message',
    'run_many_preflight'
  );
});
"""
    return app._page_shell(active_nav="Actions", body=body, extra_script=actions_script)
