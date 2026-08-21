(function () {
  function applyPatch() {
    if (typeof window.renderAccounts !== 'function' || window.renderAccounts._modePatchApplied) {
      if (window.renderAccounts && window.renderAccounts._modePatchApplied) return;
      setTimeout(applyPatch, 100);
      return;
    }
    var _orig = window.renderAccounts;
    window.renderAccounts = function (accounts, state) {
      var result = _orig.call(this, accounts, state);
      document.querySelectorAll('.acct-panel').forEach(function (panel) {
        var title = panel.querySelector('.acct-panel-title');
        if (!title || title.textContent.trim().toLowerCase() !== 'default') return;
        var badge = panel.querySelector('.acct-mode');
        if (!badge) return;
        badge.textContent = 'MANUAL · ALL';
        badge.className = badge.className.replace('acct-mode-auto', 'acct-mode-manual');
        badge.style.cssText = 'background:rgba(210,153,34,.12);color:#f0b429;border:1px solid rgba(210,153,34,.25)';
      });
      return result;
    };
    window.renderAccounts._modePatchApplied = true;
  }
  applyPatch();
})();
