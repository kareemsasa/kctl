from __future__ import annotations

from typing import Callable
from urllib.parse import urlencode

from .plan import load_plan_templates
from .paths import project_root
from .ui_dashboard_support import (
    _escape,
    _page_link,
    _provider_select_html,
    _render_collapsible_section,
    _render_selection_list,
    available_providers,
    list_plans_in_directory,
)


def _project_options(app: object) -> list[tuple[str, str]]:
    current_repo = str(app.repo_path)
    options = [(current_repo, f"Current Repo - {current_repo}")]
    seen = {current_repo}
    for project_path in app.load_tracked_projects():
        if project_path in seen:
            continue
        seen.add(project_path)
        options.append((project_path, project_path))
    return options


_TEMPLATE_OBJECTIVE_SEEDS = {
    "single_step": "Make one small, low-risk change in this repo.",
    "staged_change": "Implement a medium-risk change with separate inspect, implement, verify, and review stages.",
    "ui_validation_change": "Add or adjust validation in a UI or form workflow.",
    "tooling_change": "Modify an internal tool workflow with focused implementation and validation.",
    "review_only": "Review the relevant code or diff and produce concrete findings without making changes.",
    "commit_ready_change": "Implement a scoped change that should end in a clean, commit-ready diff.",
}


def _build_hidden_inputs(field_name: str, values: list[str]) -> str:
    return "".join(
        f"<input type='hidden' name='{_escape(field_name)}' value='{_escape(value)}'>"
        for value in values
    )


def _render_stage_nav(
    *,
    selected_plan_names: list[str],
    selected_project_paths: list[str],
    concurrency: str | None = None,
    back_stage: str | None = None,
) -> str:
    if back_stage is None:
        return ""
    params: list[tuple[str, str]] = [("stage", back_stage)]
    params.extend(("selected_plans", value) for value in selected_plan_names)
    params.extend(("project_paths", value) for value in selected_project_paths)
    if concurrency:
        params.append(("concurrency", concurrency))
    href = _page_link("/actions") + ("?" + urlencode(params) if params else "")
    return f"<div style='margin-top:12px'><a href='{_escape(href)}'>Edit Previous Step</a></div>"


def render_actions_page(
    app: object,
    *,
    action_message: str | None = None,
    summarize_preflight: Callable[..., dict[str, object]],
    selected_plan_names: list[str] | None = None,
    selected_project_paths: list[str] | None = None,
    concurrency_value: str | None = None,
    stage: str | None = None,
) -> str:
    del summarize_preflight
    templates = load_plan_templates(project_root())
    plan_templates = [
        (
            template_name,
            template.get("description") if isinstance(template, dict) else None,
            _TEMPLATE_OBJECTIVE_SEEDS.get(template_name, ""),
        )
        for template_name, template in templates.items()
    ]
    default_template_objective = plan_templates[0][2] if plan_templates else ""
    providers = available_providers()
    _plans_status, plans_message, initial_plans = list_plans_in_directory(str(app.default_plans_dir))
    project_options = _project_options(app)
    normalized_selected_plans = [name for name in (selected_plan_names or []) if name in initial_plans]
    normalized_selected_projects = [path for path in (selected_project_paths or []) if path in {value for value, _ in project_options}]
    normalized_concurrency = (concurrency_value or "1").strip() or "1"
    effective_stage = stage or "plan"
    if not normalized_selected_plans:
        effective_stage = "plan"
    elif effective_stage not in {"plan", "project", "concurrency", "provider"}:
        effective_stage = "project"
    if effective_stage in {"concurrency", "provider"} and not normalized_selected_projects:
        effective_stage = "project"
    if effective_stage == "provider" and not normalized_concurrency:
        effective_stage = "concurrency"

    initial_plans_html = _render_selection_list(
        "selected_plans",
        [(plan, plan) for plan in initial_plans],
        heading="Plans",
        selected_values=set(normalized_selected_plans),
        item_class="",
    )
    single_plan_projects_html = _render_selection_list(
        "project_paths",
        project_options,
        heading="Projects",
        selected_values=set(normalized_selected_projects or [str(app.repo_path)]),
        item_class="",
    )
    multi_plan_project_html = _render_selection_list(
        "project_paths",
        project_options,
        heading="Project",
        selected_values=set(normalized_selected_projects[:1] or [str(app.repo_path)]),
        input_type="radio",
        item_class="",
    )
    plans_empty_html = (
        "<div class='help'>"
        f"No plan files found in <code>{_escape(app.default_plans_dir)}</code>. "
        "Create one below first."
        "</div>"
    )
    notice_html = f"<div class='notice'>{_escape(action_message)}</div>" if action_message else ""
    selected_plans_inputs = _build_hidden_inputs("selected_plans", normalized_selected_plans)
    selected_projects_inputs = _build_hidden_inputs("project_paths", normalized_selected_projects)
    concurrency_input = f"<input type='hidden' name='concurrency' value='{_escape(normalized_concurrency)}'>"
    plans_summary_html = (
        f"<div class='repo-check'><strong>Saved Plan:</strong> {_escape(normalized_selected_plans[0])}</div>"
        if len(normalized_selected_plans) == 1
        else f"<div class='repo-check'><strong>Saved Plans:</strong> {_escape(', '.join(normalized_selected_plans))}</div>"
    )
    project_summary_html = (
        f"<div class='repo-check'><strong>Saved Projects:</strong> {_escape(', '.join(normalized_selected_projects))}</div>"
        if len(normalized_selected_plans) == 1 and len(normalized_selected_projects) > 1
        else f"<div class='repo-check'><strong>Saved Project:</strong> {_escape(normalized_selected_projects[0])}</div>"
        if normalized_selected_projects
        else ""
    )
    run_plans_body = (
        "<div class='help'>Plans source: "
        f"<code>{_escape(app.default_plans_dir)}</code>"
        "</div>"
    )
    if effective_stage == "plan":
        run_plans_body += (
            "<form method='get' action='/actions'>"
            "<div class='help'>Choose the plan or plans you want to run.</div>"
            f"{initial_plans_html if initial_plans else plans_empty_html}"
            "<input type='hidden' name='stage' value='project'>"
            "<button type='submit'>Save Plan Selection</button>"
            "</form>"
        )
    elif effective_stage == "project":
        project_help = (
            "Choose one or more projects to run this plan."
            if len(normalized_selected_plans) == 1
            else "Choose exactly one project to run these plans."
        )
        project_selector_html = single_plan_projects_html if len(normalized_selected_plans) == 1 else multi_plan_project_html
        run_plans_body += (
            plans_summary_html
            + "<form method='get' action='/actions'>"
            + selected_plans_inputs
            + "<div class='help'>"
            + _escape(project_help)
            + "</div>"
            + project_selector_html
            + "<input type='hidden' name='stage' value='concurrency'>"
            + "<button type='submit'>Save Project Selection</button>"
            + "</form>"
            + _render_stage_nav(
                selected_plan_names=normalized_selected_plans,
                selected_project_paths=[],
                back_stage="plan",
            )
        )
    elif effective_stage == "concurrency":
        run_plans_body += (
            plans_summary_html
            + project_summary_html
            + "<form method='get' action='/actions'>"
            + selected_plans_inputs
            + selected_projects_inputs
            + "<label for='run_plans_concurrency'><strong>Concurrency</strong></label>"
            + f"<input id='run_plans_concurrency' name='concurrency' type='number' min='1' value='{_escape(normalized_concurrency)}'>"
            + "<div class='help'>How many plans can run at the same time. Use 1 for the safest option.</div>"
            + "<input type='hidden' name='stage' value='provider'>"
            + "<button type='submit'>Save Concurrency</button>"
            + "</form>"
            + _render_stage_nav(
                selected_plan_names=normalized_selected_plans,
                selected_project_paths=normalized_selected_projects,
                back_stage="project",
            )
        )
    else:
        run_plans_body += (
            plans_summary_html
            + project_summary_html
            + "<form method='post' action='/actions/run-many' id='run_plans_form'>"
            + f"<input type='hidden' name='target_repo' value='{_escape(app.repo_path)}'>"
            + selected_plans_inputs
            + selected_projects_inputs
            + concurrency_input
            + f"<div class='repo-check'><strong>Saved Concurrency:</strong> {_escape(normalized_concurrency)}</div>"
            + "<div class='help'>Override the default provider only if this run should use a different agent.</div>"
            + (_provider_select_html("provider_override", providers) if providers else "<div class='help'>No provider overrides available.</div>")
            + "<button type='submit' id='run_plans_submit_button'>Run Plans</button>"
            + "</form>"
            + _render_stage_nav(
                selected_plan_names=normalized_selected_plans,
                selected_project_paths=normalized_selected_projects,
                concurrency=normalized_concurrency,
                back_stage="concurrency",
            )
        )
    create_plan_html = _render_collapsible_section(
        "Create Plan",
        (
            "<div class='help' style='margin-top:8px'>Creates one new plan file in the target project's plans folder.</div>"
            "<form method='post' action='/actions/create-plan'>"
            f"<label for='target_repo_create_plan'><strong>Target Repo</strong></label>"
            f"<input id='target_repo_create_plan' name='target_repo' type='text' value='{_escape(app.repo_path)}' required>"
            "<div id='target_repo_create_plan_status' class='repo-check'></div>"
            "<label for='template_name'><strong>Template</strong></label>"
            "<select id='template_name' name='template_name' "
            "onchange=\"var o=this.options[this.selectedIndex];var t=document.getElementById('objective');"
            "if(!o||!t)return;var seed=o.getAttribute('data-objective')||'';"
            "var prev=t.getAttribute('data-template-objective')||'';"
            "var cur=(t.value||'').trim();if(!cur||cur===prev){t.value=seed;}t.setAttribute('data-template-objective',seed);\">"
            + "".join(
                f"<option value='{_escape(name)}' data-objective='{_escape(objective_seed)}'>{_escape(name)}"
                + (f" - {_escape(desc)}" if desc else "")
                + "</option>"
                for name, desc, objective_seed in plan_templates
            )
            + "</select>"
            + f"<div><strong>Plan Root</strong>: <code>{_escape(app.default_plans_dir)}</code></div>"
            + "<label for='output_path'><strong>Plan File Name</strong></label>"
            + "<input id='output_path' name='output_path' type='text' placeholder='001-sample.yaml' required>"
            + "<label for='objective'><strong>Objective</strong></label>"
            + f"<textarea id='objective' name='objective' rows='5' placeholder='Describe the change' data-template-objective='{_escape(default_template_objective)}' required>{_escape(default_template_objective)}</textarea>"
            + "<label class='checkbox'><input name='force' type='checkbox' value='1'> Overwrite if the file exists</label>"
            + "<button type='submit'>Create Plan</button>"
            + "</form>"
        ),
    )
    body = (
        "<main class='single-column'>"
        + "<div class='column'>"
        + f"{notice_html}"
        + "<section class='panel'>"
        + "<h2>Refresh Index</h2>"
        + "<div class='help'>Refreshes the dashboard data from saved runs and workspaces on this machine.</div>"
        + "<form method='post' action='/actions/index'>"
        + "<button type='submit'>Refresh Index</button>"
        + "</form>"
        + "</section>"
        + "<section class='panel'>"
        + "<h2>Run Plans</h2>"
        + run_plans_body
        + "</section>"
        + create_plan_html
        + "</div>"
        + "</main>"
    )
    actions_script = """\
window.addEventListener('DOMContentLoaded', () => {
  wireCopyButtons(document);
  wireRepoCheck('target_repo_create_plan', 'target_repo_create_plan_status');
});
"""
    return app._page_shell(active_nav="Actions", body=body, extra_script=actions_script)
