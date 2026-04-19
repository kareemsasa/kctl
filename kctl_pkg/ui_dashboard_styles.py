from __future__ import annotations


_COMMON_STYLES = """\
body {
  font-family: sans-serif;
  margin: 0;
  padding: 16px;
  background: #f5f5f5;
  color: #111;
  box-sizing: border-box;
  overflow-x: hidden;
}
.page, main {
  max-width: 1400px;
  margin: 0 auto;
  box-sizing: border-box;
  width: 100%;
}
.page-header {
  background: #1f2937;
  color: white;
  border-radius: 10px;
  padding: 16px;
}
.header-path {
  opacity: 0.7;
  font-size: 0.9em;
}
.main-nav {
  display: flex;
  gap: 4px;
  margin-top: 12px;
}
.nav-link {
  padding: 8px 16px;
  border-radius: 6px;
  text-decoration: none;
  color: rgba(255,255,255,0.7);
  font-weight: 500;
  font-size: 0.95em;
}
.nav-link:hover {
  background: rgba(255,255,255,0.1);
  color: white;
}
.nav-link.active {
  background: rgba(255,255,255,0.15);
  color: white;
}
.overview-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px 20px;
  padding: 12px 16px;
  margin-top: 12px;
  background: white;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 0.95em;
}
.overview-bar span {
  white-space: nowrap;
}
main {
  display: grid;
  grid-template-columns: 300px 1fr;
  gap: 16px;
  padding: 16px 0;
  align-items: start;
  min-width: 0;
}
main.single-column {
  grid-template-columns: 1fr;
  max-width: 900px;
}
.actions-details summary {
  cursor: pointer;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 8px;
}
.actions-details summary::-webkit-details-marker {
  display: none;
}
.actions-details summary::before {
  content: "\\25B6";
  font-size: 0.7em;
  transition: transform 0.15s;
}
.actions-details[open] summary::before {
  transform: rotate(90deg);
}
.inline-heading {
  display: inline;
  margin: 0;
}
.column {
  display: flex;
  flex-direction: column;
  gap: 16px;
  min-width: 0;
}
.panel {
  background: white;
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 16px;
  min-width: 0;
}
form {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 12px;
}
input, button, textarea {
  font: inherit;
  padding: 8px 10px;
}
select {
  font: inherit;
  padding: 8px 10px;
}
button {
  cursor: pointer;
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}
textarea {
  resize: vertical;
  min-height: 120px;
}
.checkbox {
  display: flex;
  align-items: center;
  gap: 8px;
}
.selection-list-item {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  align-items: start;
  column-gap: 10px;
  margin: 4px 0;
}
.selection-list-control {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 22px;
}
.selection-list-control input {
  margin: 0;
}
.selection-list-label {
  display: block;
  min-width: 0;
  overflow-wrap: anywhere;
}
.notice {
  margin-bottom: 12px;
  padding: 10px 12px;
  border-radius: 6px;
  background: #eff6ff;
  border: 1px solid #bfdbfe;
}
.help {
  color: #4b5563;
  font-size: 0.95em;
  line-height: 1.4;
}
.repo-check {
  font-size: 0.92em;
  color: #4b5563;
}
.repo-check[data-status='ok'] {
  color: #15803d;
}
.repo-check[data-status='missing'],
.repo-check[data-status='not_dir'],
.repo-check[data-status='empty'] {
  color: #b91c1c;
}
.plans-preview {
  color: #374151;
  font-size: 0.92em;
  line-height: 1.4;
}
.plans-preview ul {
  margin: 6px 0 0;
  padding-left: 18px;
}
.plans-preview label {
  margin: 4px 0;
}
.preflight-summary {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.launch-decision {
  border-radius: 8px;
  padding: 10px 12px;
  font-weight: 700;
}
.launch-decision-pass {
  background: #dcfce7;
  color: #166534;
}
.launch-decision-warn {
  background: #fef3c7;
  color: #92400e;
}
.launch-decision-block {
  background: #fee2e2;
  color: #991b1b;
}
.preflight-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 8px;
}
.preflight-item {
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 10px;
  background: white;
}
.preflight-badge {
  display: inline-block;
  margin-left: 6px;
  padding: 1px 6px;
  border-radius: 999px;
  font-size: 0.78em;
  letter-spacing: 0.04em;
}
.preflight-badge-pass {
  background: #dcfce7;
  color: #166534;
}
.preflight-badge-warn {
  background: #fef3c7;
  color: #92400e;
}
.preflight-badge-block {
  background: #fee2e2;
  color: #991b1b;
}
.mini-button {
  margin-top: 8px;
  padding: 6px 8px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  background: #f8fafc;
  font-size: 0.85em;
}
.list-item, .card {
  display: block;
  text-decoration: none;
  color: inherit;
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 12px;
  margin-bottom: 8px;
  background: white;
  overflow-wrap: anywhere;
  min-width: 0;
}
.list-item:hover, .card:hover {
  border-color: #999;
}
.code-block {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 12px;
  margin-top: 12px;
  font-family: monospace;
  font-size: 0.9em;
}
.live-output {
  max-height: 420px;
  overflow: auto;
  background: #0f172a;
  color: #e2e8f0;
}
.status-success {
  border-left: 4px solid #15803d;
}
.status-failure {
  border-left: 4px solid #b91c1c;
}
.status-running {
  border-left: 4px solid #1d4ed8;
}
.status-neutral {
  border-left: 4px solid #6b7280;
}
table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  min-width: 760px;
}
th, td {
  text-align: left;
  padding: 8px;
  border-bottom: 1px solid #e5e7eb;
  vertical-align: top;
  overflow-wrap: anywhere;
}
.table-scroll {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  max-width: 100%;
}
.empty {
  color: #666;
  font-style: italic;
}
code {
  font-family: monospace;
  font-size: 0.95em;
}
header div, .panel div {
  overflow-wrap: anywhere;
}
label {
  overflow-wrap: anywhere;
}
.project-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  margin-bottom: 8px;
  background: white;
}
.project-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.project-name {
  font-weight: 600;
  font-size: 1em;
}
a.project-name:hover {
  text-decoration: underline !important;
}
.project-path {
  font-size: 0.85em;
  color: #666;
  overflow-wrap: anywhere;
}
.project-header form {
  margin: 0;
  flex-shrink: 0;
}
.project-git {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  font-size: 0.85em;
}
.git-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  white-space: nowrap;
}
.git-branch {
  background: #e8f0fe;
  color: #1a56db;
}
.git-clean {
  background: #ecfdf5;
  color: #166534;
}
.git-dirty {
  background: #fef2f2;
  color: #991b1b;
}
.git-ahead {
  background: #f0fdf4;
  color: #166534;
}
.git-behind {
  background: #fffbeb;
  color: #92400e;
}
.git-synced {
  background: #f3f4f6;
  color: #6b7280;
}
.git-commit {
  color: #6b7280;
  font-size: 0.9em;
}
.git-unavailable {
  color: #9ca3af;
  font-style: italic;
  font-size: 0.9em;
}
.back-link {
  display: inline-block;
  margin-bottom: 8px;
  font-size: 0.9em;
  color: #2563eb;
  text-decoration: none;
}
.back-link:hover {
  text-decoration: underline;
}
.detail-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
}
.detail-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}
@media (max-width: 700px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
.detail-section h3 {
  margin: 0 0 8px;
  font-size: 1em;
}
.remote-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 0;
  border-bottom: 1px solid #f0f0f0;
  font-size: 0.9em;
}
.remote-row:last-child {
  border-bottom: none;
}
.remote-name {
  font-weight: 600;
  min-width: 60px;
}
.remote-url {
  font-family: monospace;
  font-size: 0.92em;
  overflow-wrap: anywhere;
  flex: 1;
}
.remote-direction {
  color: #6b7280;
  font-size: 0.85em;
  min-width: 50px;
}
.ssh-status {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.85em;
}
.ssh-ok {
  background: #ecfdf5;
  color: #166534;
}
.ssh-fail {
  background: #fef2f2;
  color: #991b1b;
}
.ssh-pending {
  background: #f3f4f6;
  color: #6b7280;
}
.branch-list {
  list-style: none;
  padding: 0;
  margin: 0;
}
.branch-list li {
  padding: 4px 0;
  font-size: 0.9em;
  font-family: monospace;
}
.branch-current {
  font-weight: 600;
  color: #1a56db;
}
.commit-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.88em;
}
.commit-table th, .commit-table td {
  padding: 5px 8px;
  text-align: left;
  border-bottom: 1px solid #f0f0f0;
}
.commit-table th {
  font-weight: 600;
  color: #374151;
}
.commit-sha {
  font-family: monospace;
  color: #2563eb;
}
.status-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 4px;
  font-size: 0.82em;
  font-weight: 600;
}
.status-badge.status-success { background: #dcfce7; color: #166534; }
.status-badge.status-failure { background: #fee2e2; color: #991b1b; }
.status-badge.status-running { background: #dbeafe; color: #1d4ed8; }
.status-badge.status-neutral { background: #f3f4f6; color: #374151; }
.lifecycle-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 4px;
  font-size: 0.82em;
  font-weight: 600;
}
.lifecycle-released { background: #dcfce7; color: #166534; }
.lifecycle-active { background: #dbeafe; color: #1d4ed8; }
.lifecycle-stale { background: #fef3c7; color: #92400e; }
.kv-row {
  display: flex;
  gap: 8px;
  align-items: baseline;
  font-size: 0.92em;
  margin-top: 3px;
  overflow-wrap: anywhere;
}
.kv-label {
  color: #6b7280;
  min-width: 80px;
  flex-shrink: 0;
  font-size: 0.9em;
}
@media (max-width: 860px) {
  body {
    padding: 12px;
  }
  main {
    grid-template-columns: 1fr;
    padding: 12px 0 0;
  }
  .dashboard-primary-column {
    order: -1;
  }
  .page-header {
    padding: 14px 12px;
  }
  .panel {
    padding: 14px;
  }
  .overview-bar {
    margin-top: 10px;
  }
}
@media (max-width: 640px) {
  body {
    font-size: 15px;
    padding: 10px;
  }
  .page-header {
    padding: 10px;
  }
  main {
    gap: 12px;
    padding: 10px 0 0;
  }
  .column {
    gap: 12px;
  }
  .list-item, .card {
    padding: 10px;
  }
  input, button, select, textarea {
    width: 100%;
    box-sizing: border-box;
    min-width: 0;
  }
  table {
    min-width: 640px;
  }
}
.session-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  margin-bottom: 8px;
  background: white;
  text-decoration: none;
  color: inherit;
}
.session-item:hover {
  border-color: #999;
}
.session-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  font-size: 0.85em;
  color: #6b7280;
}
.session-prompt-preview {
  font-size: 0.92em;
  color: #374151;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
}
.session-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.85em;
  font-weight: 500;
  white-space: nowrap;
}
.session-badge-running {
  background: #dbeafe;
  color: #1d4ed8;
}
.session-badge-completed {
  background: #dcfce7;
  color: #166534;
}
.session-badge-failed {
  background: #fee2e2;
  color: #991b1b;
}
.session-badge-provider {
  background: #f3f4f6;
  color: #374151;
}
.session-output {
  max-height: 600px;
  overflow: auto;
  background: #0f172a;
  color: #e2e8f0;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  border: 1px solid #334155;
  border-radius: 6px;
  padding: 12px;
  margin-top: 12px;
  font-family: monospace;
  font-size: 0.88em;
  line-height: 1.5;
}
.session-prompt-full {
  white-space: pre-wrap;
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 12px;
  font-size: 0.92em;
  line-height: 1.5;
}
.session-actions {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}
.btn-primary {
  background: #1d4ed8;
  color: white;
  border: none;
  border-radius: 6px;
  padding: 10px 16px;
  font-weight: 500;
}
.btn-primary:hover {
  background: #1e40af;
}
.btn-danger {
  background: #dc2626;
  color: white;
  border: none;
  border-radius: 6px;
  padding: 8px 12px;
  font-size: 0.9em;
}
.btn-danger:hover {
  background: #b91c1c;
}
.btn-sm {
  background: var(--surface);
  color: var(--text);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 4px 10px;
  font-size: 0.82em;
  cursor: pointer;
  white-space: nowrap;
}
.btn-sm:hover {
  background: var(--border);
}
.btn-sm.btn-danger {
  background: #dc2626;
  color: white;
  border-color: #dc2626;
  padding: 4px 10px;
  font-size: 0.82em;
}
.btn-sm.btn-danger:hover {
  background: #b91c1c;
  border-color: #b91c1c;
}
.action-message {
  padding: 10px 14px;
  border-radius: 6px;
  margin-bottom: 12px;
  font-size: 0.9em;
}
.action-ok {
  background: rgba(34,197,94,0.12);
  border: 1px solid rgba(34,197,94,0.3);
  color: #22c55e;
}
.action-error {
  background: rgba(239,68,68,0.12);
  border: 1px solid rgba(239,68,68,0.3);
  color: #ef4444;
}
.session-chat-shell {
  padding: 0;
  overflow: hidden;
}
.session-chat-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
  padding: 16px 16px 0;
}
.session-chat-window {
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-height: min(68vh, 760px);
  overflow-y: auto;
  padding: 12px 16px 96px;
  background:
    linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
}
.session-chat-row {
  display: flex;
  width: 100%;
}
.session-chat-row-user {
  justify-content: flex-end;
}
.session-chat-row-agent {
  justify-content: flex-start;
}
.session-chat-bubble {
  max-width: min(85%, 720px);
  border-radius: 18px;
  padding: 10px 12px;
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
  overflow-wrap: anywhere;
}
.session-chat-bubble-user {
  background: #1d4ed8;
  color: #eff6ff;
  border-bottom-right-radius: 6px;
}
.session-chat-bubble-agent {
  background: white;
  color: #0f172a;
  border: 1px solid #dbe4f0;
  border-bottom-left-radius: 6px;
}
.session-chat-meta {
  display: flex;
  gap: 8px;
  justify-content: space-between;
  align-items: center;
  font-size: 0.78em;
  opacity: 0.78;
  margin-bottom: 6px;
}
.session-chat-content {
  white-space: pre-wrap;
  line-height: 1.5;
}
.session-chat-placeholder {
  color: #64748b;
  font-style: italic;
}
.session-chat-empty {
  text-align: center;
  color: #64748b;
  padding: 20px 12px;
}
.session-chat-composer {
  position: sticky;
  bottom: 0;
  padding: 12px 16px 16px;
  background: linear-gradient(180deg, rgba(238,242,255,0) 0%, #eef2ff 22%, #eef2ff 100%);
  border-top: 1px solid #dbe4f0;
}
.session-chat-form {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 10px;
  align-items: end;
  margin: 0;
}
.session-chat-form-disabled {
  opacity: 0.88;
}
.session-chat-input {
  min-height: 52px;
  max-height: 180px;
  resize: vertical;
  border-radius: 16px;
  border: 1px solid #bfdbfe;
  background: white;
  padding: 12px 14px;
}
.session-chat-send {
  min-width: 96px;
  align-self: stretch;
}
@media (max-width: 640px) {
  .session-chat-header {
    padding: 14px 14px 0;
  }
  .session-chat-window {
    max-height: calc(100vh - 280px);
    padding: 10px 14px 104px;
  }
  .session-chat-bubble {
    max-width: 92%;
  }
  .session-chat-composer {
    padding: 10px 14px max(14px, env(safe-area-inset-bottom));
  }
  .session-chat-form {
    grid-template-columns: 1fr;
  }
  .session-chat-send {
    min-width: 0;
  }
}
"""
