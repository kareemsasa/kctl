from __future__ import annotations


_SHARED_SCRIPTS = """\
function wireRepoCheck(inputId, statusId) {
  const input = document.getElementById(inputId);
  const status = document.getElementById(statusId);
  if (!input || !status) return;
  let timer = null;
  async function refreshStatus() {
    const params = new URLSearchParams({ path: input.value });
    const response = await fetch(`/api/check-repo?${params.toString()}`);
    const data = await response.json();
    status.dataset.status = data.status;
    status.textContent = data.message;
  }
  function scheduleRefresh() {
    clearTimeout(timer);
    timer = setTimeout(refreshStatus, 150);
  }
  input.addEventListener('input', scheduleRefresh);
  refreshStatus();
}
async function copyTextValue(value) {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement('textarea');
  textarea.value = value;
  textarea.setAttribute('readonly', 'readonly');
  textarea.style.position = 'fixed';
  textarea.style.top = '0';
  textarea.style.left = '0';
  textarea.style.opacity = '0';
  textarea.style.pointerEvents = 'none';
  textarea.style.zIndex = '-1';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  if (typeof textarea.setSelectionRange === 'function') {
    textarea.setSelectionRange(0, textarea.value.length);
  }
  const ok = document.execCommand('copy');
  document.body.removeChild(textarea);
  if (!ok) {
    throw new Error('copy failed');
  }
}
function extractCopyValue(node) {
  let value = node.getAttribute('data-copy') || '';
  if (!value) {
    const targetSelector = node.getAttribute('data-copy-target') || '';
    const targetNode = targetSelector ? document.querySelector(targetSelector) : null;
    if (targetNode) {
      if (
        targetNode instanceof HTMLTextAreaElement
        || targetNode instanceof HTMLInputElement
      ) {
        value = targetNode.value || '';
      } else {
        value = targetNode.textContent || '';
      }
    }
    const copyLastLines = parseInt(node.getAttribute('data-copy-last-lines') || '', 10);
    if (copyLastLines > 0) {
      const lines = value.split(/\r?\n/);
      value = lines.slice(-copyLastLines).join('\n');
    }
  }
  return value;
}
function focusCopyTarget(node) {
  if (!node) return null;
  const targetSelector = node.getAttribute('data-copy-target') || '';
  const targetNode = targetSelector ? document.querySelector(targetSelector) : null;
  if (!targetNode) return null;
  if (
    targetNode instanceof HTMLTextAreaElement
    || targetNode instanceof HTMLInputElement
  ) {
    targetNode.focus();
    targetNode.select();
    if (typeof targetNode.setSelectionRange === 'function') {
      targetNode.setSelectionRange(0, targetNode.value.length);
    }
  }
  return targetNode;
}
async function triggerCopyForNode(node) {
  if (!node) return false;
  let handling = node.dataset.copyHandling === '1';
  if (handling) return false;
  node.dataset.copyHandling = '1';
  node.setAttribute('data-label', node.getAttribute('data-label') || node.textContent || '');
  focusCopyTarget(node);
  const value = extractCopyValue(node);
  if (!value) {
    node.dataset.copyHandling = '0';
    return false;
  }
  try {
    await copyTextValue(value);
    node.textContent = 'Copied';
    window.setTimeout(() => {
      node.textContent = node.getAttribute('data-label') || '';
    }, 1200);
    return true;
  } catch (_error) {
    node.textContent = 'Selected';
    window.setTimeout(() => {
      node.textContent = node.getAttribute('data-label') || '';
    }, 1200);
    return true;
  } finally {
    window.setTimeout(() => {
      node.dataset.copyHandling = '0';
    }, 50);
  }
}
window.kctlCopyButtonClick = function(node, event) {
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (event && typeof event.stopPropagation === 'function') {
    event.stopPropagation();
  }
  triggerCopyForNode(node);
  return false;
};
window.kctlActionButtonClick = function(node, actionName, event) {
  if (!node || !actionName) return false;
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (event && typeof event.stopPropagation === 'function') {
    event.stopPropagation();
  }
  if (node.dataset.actionHandling === '1') return false;
  node.dataset.actionHandling = '1';
  window.setTimeout(() => {
    node.dataset.actionHandling = '0';
  }, 100);
  const action = window[actionName];
  if (typeof action !== 'function') return false;
  return action(node);
};
window.kctlKeyActionButton = function(node, actionName, event) {
  if (!event) return false;
  if (event.key !== 'Enter' && event.key !== ' ') return true;
  return window.kctlActionButtonClick(node, actionName, event);
};
window.kctlSubmitButtonClick = function(node, event) {
  if (!node || !node.form) return false;
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  if (node.dataset.submitHandling === '1') return false;
  node.dataset.submitHandling = '1';
  node.disabled = true;
  node.setAttribute('data-label', node.getAttribute('data-label') || node.textContent || '');
  node.textContent = 'Stopping...';
  if (typeof node.form.requestSubmit === 'function') {
    node.form.requestSubmit();
  } else {
    node.form.submit();
  }
  return false;
};
function wireCopyButtons(root) {
  (root || document).querySelectorAll('[data-copy], [data-copy-target]').forEach((node) => {
    if (node.dataset.bound === '1') return;
    node.dataset.bound = '1';
    node.setAttribute('data-label', node.textContent || '');
    const handleCopy = async (event) => {
      event.preventDefault();
      if (typeof event.stopPropagation === 'function') {
        event.stopPropagation();
      }
      await triggerCopyForNode(node);
    };
    node.addEventListener('click', handleCopy);
    node.addEventListener('touchstart', handleCopy, { passive: false });
    node.addEventListener('touchend', handleCopy, { passive: false });
    node.addEventListener('mousedown', handleCopy);
    if (window.PointerEvent) {
      node.addEventListener('pointerdown', handleCopy);
      node.addEventListener('pointerup', handleCopy);
    }
  });
}
"""
