"""
Dashboard HTML generators for LOV3 Houston analytics.

Each function returns a self-contained HTML page (no external dependencies).
These are pure string generators with no imports needed.
"""

def _bank_review_html() -> str:
    """Return self-contained HTML for the bank transaction review dashboard."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Bank Transaction Review</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}
.header{background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);color:#fff;padding:24px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.header h1{font-size:1.5rem;font-weight:700;letter-spacing:0.5px}
.header .subtitle{font-size:0.85rem;opacity:0.7}
.container{max-width:1400px;margin:0 auto;padding:24px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.kpi-card .label{font-size:0.8rem;text-transform:uppercase;letter-spacing:0.5px;color:#666;margin-bottom:4px}
.kpi-card .value{font-size:1.8rem;font-weight:700}
.kpi-card .value.warn{color:#e74c3c}
.kpi-card .value.ok{color:#27ae60}
.kpi-card .value.info{color:#2980b9}
.filter-bar{background:#fff;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.filter-bar .field{display:flex;flex-direction:column;gap:4px}
.filter-bar .field label{font-size:0.75rem;font-weight:600;color:#555;text-transform:uppercase}
.filter-bar input,.filter-bar select{padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:0.875rem;background:#fff}
.filter-bar input:focus,.filter-bar select:focus{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,0.1)}
.filter-bar button{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.btn-primary{background:#6366f1;color:#fff}.btn-primary:hover{background:#4f46e5}
.btn-success{background:#10b981;color:#fff}.btn-success:hover{background:#059669}
.btn-secondary{background:#e5e7eb;color:#374151}.btn-secondary:hover{background:#d1d5db}
.table-wrap{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);overflow:hidden}
.table-info{padding:12px 20px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center;font-size:0.85rem;color:#666;flex-wrap:wrap;gap:8px}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
thead{background:#f8f9fa}
th{padding:10px 14px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
tr:hover{background:#f8f9ff}
.amount{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;text-align:right;white-space:nowrap}
.amount.debit{color:#e74c3c}
.amount.credit{color:#27ae60}
.badge{display:inline-block;padding:2px 8px;border-radius:9999px;font-size:0.7rem;font-weight:600;text-transform:uppercase}
.badge-uncat{background:#fee2e2;color:#991b1b}
.badge-auto{background:#dbeafe;color:#1e40af}
.badge-manual{background:#d1fae5;color:#065f46}
td select{padding:6px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:0.8rem;max-width:260px;width:100%}
td input[type="text"]{padding:6px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:0.8rem;width:140px}
td input[type="checkbox"]{width:16px;height:16px;cursor:pointer}
.row-save{padding:4px 12px;border:none;border-radius:6px;font-size:0.75rem;font-weight:600;cursor:pointer;background:#6366f1;color:#fff;transition:all 0.15s}
.row-save:hover{background:#4f46e5}
.row-save:disabled{background:#c7c9d1;cursor:default}
.pagination{display:flex;justify-content:center;align-items:center;gap:12px;padding:16px;flex-wrap:wrap}
.pagination button{padding:8px 16px;border:1px solid #d1d5db;border-radius:8px;background:#fff;cursor:pointer;font-size:0.85rem;transition:all 0.15s}
.pagination button:hover:not(:disabled){background:#f3f4f6}
.pagination button:disabled{opacity:0.4;cursor:default}
.toast-container{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px}
.toast{padding:12px 20px;border-radius:8px;color:#fff;font-size:0.875rem;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.15);animation:slideIn 0.3s ease}
.toast.success{background:#10b981}
.toast.error{background:#ef4444}
.toast.info{background:#6366f1}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #fff;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.2);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#fff;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.15);text-align:center}
.desc-cell{max-width:480px;word-break:break-word;white-space:normal;font-size:0.8rem;line-height:1.3}
.rule-kw{padding:4px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:0.75rem;width:120px;display:none}
@media(max-width:768px){.container{padding:12px}.header{padding:16px}.kpi-row{grid-template-columns:1fr 1fr}.filter-bar{flex-direction:column}}
.upload-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.upload-card .upload-label{font-size:0.85rem;font-weight:600;color:#374151}
.upload-card input[type="file"]{font-size:0.85rem}
.upload-card .btn-upload{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;background:#6366f1;color:#fff;transition:all 0.15s}
.upload-card .btn-upload:hover{background:#4f46e5}
.upload-card .btn-upload:disabled{background:#c7c9d1;cursor:default}
.upload-result{margin-top:8px;padding:12px 16px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;font-size:0.82rem;color:#166534;display:none;width:100%}
.upload-result .result-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px;margin-top:8px}
.upload-result .result-item{font-size:0.8rem}.upload-result .result-item strong{color:#15803d}
.nav-bar{background:#fff;border-bottom:1px solid #e5e7eb;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#374151;transition:all 0.15s}
.nav-bar a:hover{background:#f3f4f6}
.nav-bar a.active{background:#6366f1;color:#fff}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>LOV3 Bank Transaction Review</h1>
    <div class="subtitle">Categorize uncategorized bank transactions and create auto-categorization rules</div>
  </div>
  <button class="btn-success" onclick="saveAll()" id="saveAllBtn" style="padding:10px 24px;border:none;border-radius:8px;font-size:0.9rem;font-weight:700;cursor:pointer;color:#fff">Save All Changes</button>
</div>
<div class="nav-bar">
  <a href="/bank-review" class="active">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="kpi-row">
    <div class="kpi-card"><div class="label">Uncategorized</div><div class="value warn" id="kpiUncat">--</div></div>
    <div class="kpi-card"><div class="label">Uncategorized $</div><div class="value warn" id="kpiUncatAmt">--</div></div>
    <div class="kpi-card"><div class="label">Categorized</div><div class="value ok" id="kpiCat">--</div></div>
    <div class="kpi-card"><div class="label">Total Transactions</div><div class="value info" id="kpiTotal">--</div></div>
  </div>

  <div id="uploadInfo" style="display:none;background:#1a1a2e;border:1px solid #333;border-radius:10px;padding:12px 20px;margin-bottom:16px;font-size:0.82rem;color:#9ca3af;align-items:center;gap:24px;flex-wrap:wrap">
    <span><strong style="color:#a5b4fc">Last Upload:</strong> <span id="infoUploadDate">--</span></span>
    <span><strong style="color:#a5b4fc">File:</strong> <span id="infoUploadFile">--</span></span>
    <span><strong style="color:#a5b4fc">Newest Transaction:</strong> <span id="infoNewestTxn">--</span></span>
    <span><strong style="color:#a5b4fc">Oldest Transaction:</strong> <span id="infoOldestTxn">--</span></span>
  </div>

  <div class="upload-card">
    <span class="upload-label">Upload BofA CSV:</span>
    <input type="file" id="csvFile" accept=".csv">
    <button class="btn-upload" id="uploadBtn" onclick="window._uploadCSV()">Upload CSV</button>
    <div class="upload-result" id="uploadResult"></div>
  </div>

  <div class="filter-bar">
    <div class="field">
      <label>Search</label>
      <input type="text" id="filterSearch" placeholder="Description..." onkeydown="if(event.key==='Enter')applyFilters()">
    </div>
    <div class="field">
      <label>From</label>
      <input type="date" id="filterFrom">
    </div>
    <div class="field">
      <label>To</label>
      <input type="date" id="filterTo">
    </div>
    <div class="field">
      <label>Status</label>
      <select id="filterStatus">
        <option value="uncategorized">Uncategorized</option>
        <option value="all">All</option>
        <option value="categorized">Categorized</option>
      </select>
    </div>
    <div class="field">
      <label>Sort</label>
      <select id="filterSort">
        <option value="date_desc">Date (newest)</option>
        <option value="date_asc">Date (oldest)</option>
        <option value="amount_desc">Amount (highest)</option>
        <option value="amount_asc">Amount (lowest)</option>
      </select>
    </div>
    <button class="btn-primary" onclick="applyFilters()">Filter</button>
    <button class="btn-secondary" onclick="resetFilters()">Reset</button>
  </div>

  <div class="table-wrap">
    <div class="table-info" style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <span id="tableInfo">Loading...</span>
      <span id="changeCount" style="font-weight:600;color:#6366f1"></span>
      <button id="deleteSelBtn" onclick="deleteSelected()" style="display:none;padding:6px 16px;border:none;border-radius:6px;font-size:0.82rem;font-weight:700;cursor:pointer;color:#fff;background:#ef4444">Delete Selected (<span id="delCount">0</span>)</button>
    </div>
    <div style="overflow-x:auto">
    <table>
      <thead>
        <tr>
          <th style="width:36px"><input type="checkbox" id="selectAll" onchange="toggleSelectAll(this.checked)" title="Select all"></th>
          <th>Date</th>
          <th>Description</th>
          <th style="text-align:right">Amount</th>
          <th>Current</th>
          <th>New Category</th>
          <th>Vendor</th>
          <th>Rule?</th>
          <th>Rule Keyword</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="txnBody"></tbody>
    </table>
    </div>
    <div class="pagination">
      <button onclick="prevPage()" id="btnPrev" disabled>&laquo; Previous</button>
      <span id="pageInfo"></span>
      <button onclick="nextPage()" id="btnNext" disabled>Next &raquo;</button>
    </div>
  </div>
</div>

<div class="toast-container" id="toasts"></div>
<div class="loading-overlay" id="loadingOverlay"><div class="loading-box"><div class="spinner" style="border-color:#6366f1;border-top-color:transparent;width:32px;height:32px;margin:0 auto 12px"></div><div>Loading...</div></div></div>

<script>
(function(){
  let transactions = [];
  let categories = [];
  let currentOffset = 0;
  const PAGE_SIZE = 50;
  let filteredCount = 0;
  let pendingChanges = {};  // idx -> {new_category, vendor_normalized, create_rule, rule_keyword}
  let selectedForDelete = new Set();  // indices selected for deletion

  function $(id){ return document.getElementById(id); }

  function showToast(msg, type){
    const el = document.createElement('div');
    el.className = 'toast ' + type;
    el.textContent = msg;
    $('toasts').appendChild(el);
    setTimeout(() => el.remove(), 4000);
  }

  function showLoading(on){
    $('loadingOverlay').classList.toggle('active', on);
  }

  function fmt$(n){
    return '$' + Math.abs(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  }

  function buildCatOptions(selected){
    let opts = '<option value="">-- Select --</option>';
    categories.forEach(c => {
      opts += '<option value="' + escHtml(c) + '"' + (c===selected?' selected':'') + '>' + escHtml(c) + '</option>';
    });
    return opts;
  }

  function escHtml(s){
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function badgeFor(src){
    if(src==='uncategorized') return '<span class="badge badge-uncat">Uncat</span>';
    if(src==='manual') return '<span class="badge badge-manual">Manual</span>';
    return '<span class="badge badge-auto">Auto</span>';
  }

  function suggestKeyword(desc){
    // Take first 2-3 significant words as default keyword
    const words = desc.toUpperCase().replace(/[^A-Z0-9\\s]/g,' ').split(/\\s+/).filter(w=>w.length>2);
    return words.slice(0,3).join(' ');
  }

  function renderTable(){
    const tbody = $('txnBody');
    let html = '';
    transactions.forEach((t, i) => {
      const isDebit = t.amount < 0;
      const change = pendingChanges[i];
      const ruleChecked = change && change.create_rule;
      const sel = selectedForDelete.has(i);
      html += '<tr data-idx="'+i+'">'
        + '<td style="text-align:center"><input type="checkbox" '+(sel?'checked':'')+' onchange="window._onSelectDelete('+i+',this.checked)"></td>'
        + '<td style="white-space:nowrap">'+escHtml(t.transaction_date)+'</td>'
        + '<td class="desc-cell" title="'+escHtml(t.description)+'">'+escHtml(t.description)+'</td>'
        + '<td class="amount '+(isDebit?'debit':'credit')+'">'+( isDebit?'-':'+' )+fmt$(t.amount)+'</td>'
        + '<td>'+badgeFor(t.category_source)+' '+escHtml(t.category)+'</td>'
        + '<td><select onchange="window._onCatChange('+i+',this.value)">'+buildCatOptions(change?change.new_category:'')+'</select></td>'
        + '<td><input type="text" value="'+escHtml(change?change.vendor_normalized:t.vendor_normalized)+'" onchange="window._onVendorChange('+i+',this.value)"></td>'
        + '<td style="text-align:center"><input type="checkbox" '+(ruleChecked?'checked':'')+' onchange="window._onRuleToggle('+i+',this.checked)"></td>'
        + '<td><input type="text" class="rule-kw" id="ruleKw'+i+'" value="'+escHtml(change&&change.rule_keyword?change.rule_keyword:suggestKeyword(t.description))+'" style="display:'+(ruleChecked?'inline-block':'none')+'" onchange="window._onRuleKwChange('+i+',this.value)"></td>'
        + '<td><button class="row-save" onclick="window._saveSingle('+i+')" '+(change?'':'disabled')+'>Save</button></td>'
        + '</tr>';
    });
    if(!transactions.length){
      html = '<tr><td colspan="10" style="text-align:center;padding:40px;color:#999">No transactions found</td></tr>';
    }
    tbody.innerHTML = html;
    updateChangeCount();
    updateDeleteBtn();
  }

  function updateChangeCount(){
    const n = Object.keys(pendingChanges).length;
    $('changeCount').textContent = n ? n + ' pending change' + (n>1?'s':'') : '';
  }

  // --- Event handlers exposed to inline handlers ---
  window._onCatChange = function(i, val){
    if(!val){ delete pendingChanges[i]; }
    else {
      if(!pendingChanges[i]) pendingChanges[i] = {vendor_normalized: transactions[i].vendor_normalized, create_rule:false, rule_keyword:suggestKeyword(transactions[i].description)};
      pendingChanges[i].new_category = val;
    }
    renderTable();
  };

  window._onVendorChange = function(i, val){
    if(pendingChanges[i]) pendingChanges[i].vendor_normalized = val;
  };

  window._onRuleToggle = function(i, checked){
    if(pendingChanges[i]){
      pendingChanges[i].create_rule = checked;
      const kwEl = $('ruleKw'+i);
      if(kwEl) kwEl.style.display = checked ? 'inline-block' : 'none';
    }
  };

  window._onRuleKwChange = function(i, val){
    if(pendingChanges[i]) pendingChanges[i].rule_keyword = val;
  };

  window._saveSingle = async function(i){
    const t = transactions[i];
    const c = pendingChanges[i];
    if(!c || !c.new_category) return;
    const payload = {updates:[{
      transaction_date: t.transaction_date,
      description: t.description,
      amount: t.amount,
      new_category: c.new_category,
      vendor_normalized: c.vendor_normalized || t.vendor_normalized,
      create_rule: !!c.create_rule,
      rule_keyword: c.rule_keyword || ''
    }]};
    try{
      const resp = await fetch('/api/bank-transactions/categorize',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
      const result = await resp.json();
      if(!resp.ok) throw new Error(result.error||'Update failed');
      showToast('Updated: ' + t.description.substring(0,30) + '...', 'success');
      delete pendingChanges[i];
      loadData();
    }catch(e){
      showToast('Error: '+e.message, 'error');
    }
  };

  window.saveAll = async function(){
    const keys = Object.keys(pendingChanges);
    if(!keys.length){ showToast('No changes to save','info'); return; }
    const updates = keys.map(i => {
      const t = transactions[i];
      const c = pendingChanges[i];
      return {
        transaction_date: t.transaction_date,
        description: t.description,
        amount: t.amount,
        new_category: c.new_category,
        vendor_normalized: c.vendor_normalized || t.vendor_normalized,
        create_rule: !!c.create_rule,
        rule_keyword: c.rule_keyword || ''
      };
    }).filter(u => u.new_category);

    if(!updates.length){ showToast('No valid changes','info'); return; }

    const btn = $('saveAllBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>Saving...';
    try{
      const resp = await fetch('/api/bank-transactions/categorize',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates})});
      const result = await resp.json();
      if(!resp.ok) throw new Error(result.error||'Update failed');
      const msg = result.rows_updated + ' row(s) updated' + (result.rules_created ? ', ' + result.rules_created + ' rule(s) created' : '');
      showToast(msg, 'success');
      if(result.errors && result.errors.length) result.errors.forEach(e => showToast(e, 'error'));
      pendingChanges = {};
      loadData();
    }catch(e){
      showToast('Error: '+e.message, 'error');
    }finally{
      btn.disabled = false;
      btn.textContent = 'Save All Changes';
    }
  };

  function updateDeleteBtn(){
    const n = selectedForDelete.size;
    const btn = $('deleteSelBtn');
    $('delCount').textContent = n;
    btn.style.display = n ? 'inline-block' : 'none';
    const sa = $('selectAll');
    if(sa) sa.checked = transactions.length > 0 && n === transactions.length;
  }

  window._onSelectDelete = function(i, checked){
    if(checked) selectedForDelete.add(i); else selectedForDelete.delete(i);
    updateDeleteBtn();
  };

  window.toggleSelectAll = function(checked){
    selectedForDelete.clear();
    if(checked) transactions.forEach((_, i) => selectedForDelete.add(i));
    renderTable();
  };

  window.deleteSelected = async function(){
    const n = selectedForDelete.size;
    if(!n) return;
    if(!confirm('Delete ' + n + ' transaction(s)? This cannot be undone.')) return;
    const deletes = Array.from(selectedForDelete).map(i => ({
      transaction_date: transactions[i].transaction_date,
      description: transactions[i].description,
      amount: transactions[i].amount,
    }));
    const btn = $('deleteSelBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>Deleting...';
    try{
      const resp = await fetch('/api/bank-transactions/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({deletes})});
      const result = await resp.json();
      if(!resp.ok) throw new Error(result.error||'Delete failed');
      showToast(result.rows_deleted + ' row(s) deleted', 'success');
      if(result.errors && result.errors.length) result.errors.forEach(e => showToast(e, 'error'));
      selectedForDelete.clear();
      pendingChanges = {};
      loadData();
    }catch(e){
      showToast('Error: '+e.message, 'error');
    }finally{
      btn.disabled = false;
      updateDeleteBtn();
    }
  };

  window.applyFilters = function(){ currentOffset = 0; loadData(); };
  window.resetFilters = function(){
    $('filterSearch').value = '';
    $('filterFrom').value = '';
    $('filterTo').value = '';
    $('filterStatus').value = 'uncategorized';
    $('filterSort').value = 'date_desc';
    currentOffset = 0;
    pendingChanges = {};
    loadData();
  };
  window.prevPage = function(){ currentOffset = Math.max(0, currentOffset - PAGE_SIZE); loadData(); };
  window.nextPage = function(){ currentOffset += PAGE_SIZE; loadData(); };

  window._uploadCSV = async function(){
    const fileInput = $('csvFile');
    if(!fileInput.files.length){ showToast('Please select a CSV file','error'); return; }
    const btn = $('uploadBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>Uploading...';
    $('uploadResult').style.display = 'none';
    try{
      const fd = new FormData();
      fd.append('file', fileInput.files[0]);
      const resp = await fetch('/upload-bank-csv',{method:'POST',body:fd});
      const result = await resp.json();
      if(!resp.ok) throw new Error(result.error||'Upload failed');
      showToast('Uploaded ' + result.rows_loaded + ' transactions','success');
      // Show result summary
      let catHtml = '';
      if(result.transactions_by_category){
        Object.entries(result.transactions_by_category).forEach(function(e){
          catHtml += '<div class="result-item"><strong>'+escHtml(e[0])+':</strong> $'+Math.abs(e[1]).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})+'</div>';
        });
      }
      $('uploadResult').innerHTML = '<strong>'+result.rows_loaded+' rows loaded</strong> &mdash; '+escHtml(result.date_range||'')
        +'<br>Debits: $'+(result.total_debits||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})
        +' &nbsp;|&nbsp; Credits: $'+(result.total_credits||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})
        +(catHtml ? '<div class="result-grid">'+catHtml+'</div>' : '');
      $('uploadResult').style.display = 'block';
      fileInput.value = '';
      loadData();
    }catch(e){
      showToast('Upload error: '+e.message, 'error');
    }finally{
      btn.disabled = false;
      btn.textContent = 'Upload CSV';
    }
  };

  async function loadData(){
    selectedForDelete.clear();
    showLoading(true);
    const params = new URLSearchParams({
      status: $('filterStatus').value,
      limit: PAGE_SIZE,
      offset: currentOffset,
      sort: $('filterSort').value,
      search: $('filterSearch').value,
      date_from: $('filterFrom').value,
      date_to: $('filterTo').value,
    });
    try{
      const resp = await fetch('/api/bank-transactions?' + params);
      const data = await resp.json();
      if(!resp.ok) throw new Error(data.error||'Load failed');

      // KPI
      $('kpiUncat').textContent = data.summary.uncategorized_count.toLocaleString();
      $('kpiUncatAmt').textContent = fmt$(data.summary.uncategorized_total);
      $('kpiCat').textContent = data.summary.categorized_count.toLocaleString();
      $('kpiTotal').textContent = data.summary.total_count.toLocaleString();

      // Upload info bar
      const s = data.summary;
      if(s.last_upload_date || s.newest_transaction_date){
        $('uploadInfo').style.display='flex';
        $('infoUploadDate').textContent = s.last_upload_date || 'N/A';
        $('infoUploadFile').textContent = s.last_upload_file || 'N/A';
        $('infoNewestTxn').textContent = s.newest_transaction_date || 'N/A';
        $('infoOldestTxn').textContent = s.oldest_transaction_date || 'N/A';
      }

      categories = data.categories || [];
      transactions = data.transactions || [];
      filteredCount = data.filtered_count || 0;
      pendingChanges = {};

      // Table info
      const from = currentOffset + 1;
      const to = Math.min(currentOffset + PAGE_SIZE, filteredCount);
      $('tableInfo').textContent = filteredCount ? ('Showing ' + from + '-' + to + ' of ' + filteredCount) : 'No transactions match filters';
      $('pageInfo').textContent = 'Page ' + (Math.floor(currentOffset/PAGE_SIZE)+1) + ' of ' + Math.max(1,Math.ceil(filteredCount/PAGE_SIZE));
      $('btnPrev').disabled = currentOffset === 0;
      $('btnNext').disabled = currentOffset + PAGE_SIZE >= filteredCount;

      renderTable();
    }catch(e){
      showToast('Error loading data: '+e.message, 'error');
    }finally{
      showLoading(false);
    }
  }

  // Initial load
  loadData();
})();
</script>
</body>
</html>'''



def _pnl_dashboard_html() -> str:
    """Return self-contained HTML for the P&L summary dashboard."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 P&amp;L Summary</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}
.header{background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);color:#fff;padding:24px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.header h1{font-size:1.5rem;font-weight:700;letter-spacing:0.5px}
.header .subtitle{font-size:0.85rem;opacity:0.7}
.nav-bar{background:#fff;border-bottom:1px solid #e5e7eb;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#374151;transition:all 0.15s}
.nav-bar a:hover{background:#f3f4f6}
.nav-bar a.active{background:#6366f1;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.kpi-card .label{font-size:0.8rem;text-transform:uppercase;letter-spacing:0.5px;color:#666;margin-bottom:4px}
.kpi-card .value{font-size:1.8rem;font-weight:700}
.kpi-card .value.ok{color:#27ae60}
.kpi-card .value.warn{color:#e74c3c}
.kpi-card .value.info{color:#2980b9}
.filter-bar{background:#fff;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.filter-bar .field{display:flex;flex-direction:column;gap:4px}
.filter-bar .field label{font-size:0.75rem;font-weight:600;color:#555;text-transform:uppercase}
.filter-bar input{padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:0.875rem;background:#fff}
.filter-bar input:focus{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,0.1)}
.filter-bar button{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.btn-primary{background:#6366f1;color:#fff}.btn-primary:hover{background:#4f46e5}
.section{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;overflow:hidden}
.section-header{padding:16px 20px;border-bottom:1px solid #e5e7eb;font-size:1rem;font-weight:700;color:#1a1a2e}
.section-body{padding:20px}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
thead{background:#f8f9fa}
th{padding:10px 14px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
tr:hover{background:#f8f9ff}
.amount{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
tfoot td{font-weight:700;border-top:2px solid #e5e7eb}
.warning-banner{background:#fef3c7;border:1px solid #f59e0b;border-radius:12px;padding:16px 20px;margin-bottom:24px;font-size:0.85rem;color:#92400e;display:none}
.pct-bar-wrap{display:flex;align-items:center;gap:12px}
.pct-bar{height:20px;border-radius:4px;min-width:2px}
.pct-label{font-size:0.8rem;font-weight:600;white-space:nowrap;min-width:48px}
.metric-row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0}
.metric-row:last-child{border-bottom:none}
.metric-name{font-size:0.85rem;font-weight:500;color:#374151}
.metric-val{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;font-size:0.9rem;font-weight:700}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.2);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#fff;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.15);text-align:center}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #6366f1;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
.toast-container{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px}
.toast{padding:12px 20px;border-radius:8px;color:#fff;font-size:0.875rem;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.15);animation:slideIn 0.3s ease}
.toast.success{background:#10b981}.toast.error{background:#ef4444}.toast.info{background:#6366f1}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
.hidden{display:none}
@media(max-width:768px){.container{padding:12px}.header{padding:16px}.kpi-row{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>LOV3 P&amp;L Summary</h1>
    <div class="subtitle">Revenue, expenses, and profitability for a selected date range</div>
  </div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl" class="active">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <div class="field">
      <label>Start Date</label>
      <input type="date" id="startDate">
    </div>
    <div class="field">
      <label>End Date</label>
      <input type="date" id="endDate">
    </div>
    <button class="btn-primary" onclick="loadReport()">Load Report</button>
  </div>

  <div class="warning-banner" id="noBankWarning">
    <strong>Warning:</strong> No bank transaction data found for this period. Expense and profitability figures will be incomplete.
  </div>

  <div id="reportContent" class="hidden">

    <div class="kpi-row" id="kpiRow">
      <div class="kpi-card"><div class="label">Adjusted Revenue</div><div class="value info" id="kpiRevenue">--</div></div>
      <div class="kpi-card"><div class="label">Net Profit</div><div class="value ok" id="kpiProfit">--</div></div>
      <div class="kpi-card"><div class="label">Margin %</div><div class="value info" id="kpiMargin">--</div></div>
      <div class="kpi-card"><div class="label">Prime Cost %</div><div class="value warn" id="kpiPrime">--</div></div>
      <div class="kpi-card"><div class="label">Orders</div><div class="value info" id="kpiOrders">--</div></div>
    </div>

    <!-- Revenue Breakdown -->
    <div class="section" id="revenueSection">
      <div class="section-header">Revenue Breakdown</div>
      <div class="section-body" id="revenueBody"></div>
    </div>

    <!-- Expense Breakdown -->
    <div class="section" id="expenseSection">
      <div class="section-header">Expense Breakdown</div>
      <div class="section-body" id="expenseBody"></div>
    </div>

    <!-- Cash Control -->
    <div class="section" id="cashSection">
      <div class="section-header">Cash Control</div>
      <div class="section-body" id="cashBody"></div>
    </div>

    <!-- Profitability -->
    <div class="section" id="profitSection">
      <div class="section-header">Profitability Metrics</div>
      <div class="section-body" id="profitBody"></div>
    </div>

  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Loading P&amp;L data&hellip;</div>
</div>
<div class="toast-container" id="toastContainer"></div>

<script>
(function(){
  var $ = function(id){return document.getElementById(id)};

  function fmt(n){
    if(n==null) return '--';
    return n.toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:0,maximumFractionDigits:0});
  }
  function fmtD(n){
    if(n==null) return '--';
    return n.toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:2});
  }
  function pct(n){return n!=null?(n.toFixed(1)+'%'):'--'}
  function showToast(msg,type){
    var c=$('toastContainer'),d=document.createElement('div');
    d.className='toast '+(type||'info');d.textContent=msg;c.appendChild(d);
    setTimeout(function(){d.remove()},4000);
  }

  // Default dates: first of current month -> today
  var today=new Date();
  var y=today.getFullYear(),m=today.getMonth();
  $('startDate').value=y+'-'+String(m+1).padStart(2,'0')+'-01';
  $('endDate').value=today.toISOString().slice(0,10);

  window.loadReport=function(){
    var sd=$('startDate').value, ed=$('endDate').value;
    if(!sd||!ed){showToast('Please select both dates','error');return;}
    $('loadingOverlay').classList.add('active');
    $('reportContent').classList.add('hidden');

    fetch('/profit-summary',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({start_date:sd,end_date:ed})
    })
    .then(function(r){
      if(!r.ok) return r.json().then(function(e){throw new Error(e.error||'Request failed')});
      return r.json();
    })
    .then(function(d){
      renderReport(d);
      $('reportContent').classList.remove('hidden');
    })
    .catch(function(e){showToast(e.message,'error')})
    .finally(function(){$('loadingOverlay').classList.remove('active')});
  };

  function renderReport(d){
    var rev=d.revenue||{}, exp=d.expenses||{}, cash=d.cash_control||{}, prof=d.profitability||{};

    // Warning banner
    if(!d.has_bank_data){$('noBankWarning').style.display='block';}else{$('noBankWarning').style.display='none';}

    // KPIs
    $('kpiRevenue').textContent=fmt(rev.adjusted_net_revenue);
    var np=prof.net_profit_bank_only;
    $('kpiProfit').textContent=fmt(np);
    $('kpiProfit').className='value '+(np>=0?'ok':'warn');
    $('kpiMargin').textContent=pct(prof.margin_pct_bank_only);
    $('kpiPrime').textContent=pct(prof.prime_cost_pct);
    $('kpiOrders').textContent=rev.order_count!=null?rev.order_count.toLocaleString():'--';

    // Revenue breakdown
    var rh='<table><tbody>';
    rh+='<tr><td>Net Sales</td><td class="amount">'+fmtD(rev.net_sales)+'</td></tr>';
    rh+='<tr><td>Tax</td><td class="amount">'+fmtD(rev.tax)+'</td></tr>';
    rh+='<tr><td>Tips (100% to staff)</td><td class="amount">'+fmtD(rev.tips)+'</td></tr>';
    rh+='<tr><td>Gratuity Total</td><td class="amount">'+fmtD(rev.gratuity)+'</td></tr>';
    rh+='<tr><td>&nbsp;&nbsp;&nbsp;Retained by LOV3 (35%)</td><td class="amount">'+fmtD(rev.gratuity_retained_by_lov3)+'</td></tr>';
    rh+='<tr><td>&nbsp;&nbsp;&nbsp;Paid to Staff (65%)</td><td class="amount">'+fmtD(rev.gratuity_paid_to_staff)+'</td></tr>';
    rh+='<tr><td>Total Pass-Through to Staff</td><td class="amount">'+fmtD(rev.total_pass_through_to_staff)+'</td></tr>';
    rh+='</tbody><tfoot><tr><td>Adjusted Net Revenue</td><td class="amount">'+fmtD(rev.adjusted_net_revenue)+'</td></tr></tfoot></table>';
    $('revenueBody').innerHTML=rh;

    // Expense breakdown
    var cats=exp.by_category||{};
    var sorted=Object.keys(cats).map(function(k){return{cat:k,amt:cats[k]}}).sort(function(a,b){return Math.abs(b.amt)-Math.abs(a.amt)});
    var adjRev=rev.adjusted_net_revenue||1;
    var eh='<table><thead><tr><th>Category</th><th class="text-right">Amount</th><th class="text-right">% of Revenue</th></tr></thead><tbody>';
    sorted.forEach(function(row){
      var p=Math.abs(row.amt)/adjRev*100;
      eh+='<tr><td>'+row.cat+'</td><td class="amount">'+fmtD(Math.abs(row.amt))+'</td><td class="amount">'+p.toFixed(1)+'%</td></tr>';
    });
    eh+='</tbody><tfoot>';
    eh+='<tr><td>Total Expenses (Gross)</td><td class="amount">'+fmtD(exp.total_expenses_gross)+'</td><td></td></tr>';
    eh+='<tr><td>Less: Pass-Through</td><td class="amount">('+fmtD(exp.less_pass_through)+')</td><td></td></tr>';
    eh+='<tr><td>Total Expenses (Adjusted)</td><td class="amount">'+fmtD(exp.total_expenses_adjusted)+'</td><td></td></tr>';
    eh+='</tfoot></table>';
    $('expenseBody').innerHTML=eh;

    // Cash control
    var dr=cash.drawer_activity||{};
    var ch='<div class="metric-row"><span class="metric-name">Toast Cash Collected ('+((cash.toast_cash_txn_count||0))+' txns)</span><span class="metric-val">'+fmtD(cash.toast_cash_collected)+'</span></div>';
    ch+='<div class="metric-row"><span class="metric-name">Bank Cash Deposited</span><span class="metric-val">'+fmtD(cash.bank_cash_deposited)+'</span></div>';
    ch+='<div class="metric-row"><span class="metric-name">Undeposited Cash</span><span class="metric-val" style="color:'+(cash.undeposited_cash>0?'#e74c3c':'#27ae60')+'">'+fmtD(cash.undeposited_cash)+'</span></div>';
    if(dr.drawer_collected!=null){
      ch+='<div style="border-top:2px solid #e5e7eb;margin-top:8px;padding-top:8px">';
      ch+='<div class="metric-row"><span class="metric-name">Drawer Collected</span><span class="metric-val">'+fmtD(dr.drawer_collected)+'</span></div>';
      ch+='<div class="metric-row"><span class="metric-name">Payouts</span><span class="metric-val">'+fmtD(dr.payouts)+'</span></div>';
      ch+='<div class="metric-row"><span class="metric-name">Overages</span><span class="metric-val">'+fmtD(dr.overages)+'</span></div>';
      ch+='<div class="metric-row"><span class="metric-name">Shortages</span><span class="metric-val">'+fmtD(dr.shortages)+'</span></div>';
      ch+='<div class="metric-row"><span class="metric-name">No-Sale Count</span><span class="metric-val">'+(dr.no_sale_count||0)+'</span></div>';
      ch+='<div class="metric-row"><span class="metric-name">Exact Close-Outs</span><span class="metric-val">'+(dr.exact_closeouts||0)+'</span></div>';
      ch+='</div>';
    }
    $('cashBody').innerHTML=ch;

    // Profitability
    var metrics=[
      {name:'COGS',val:prof.cogs_total,p:prof.cogs_pct,color:'#ef4444'},
      {name:'Labor (True)',val:prof.labor_true,p:prof.labor_pct,color:'#f59e0b'},
      {name:'Prime Cost',val:prof.prime_cost,p:prof.prime_cost_pct,color:'#8b5cf6'},
      {name:'Marketing',val:prof.marketing_total,p:prof.marketing_pct,color:'#3b82f6'},
      {name:'OPEX',val:prof.opex_total,p:prof.opex_pct,color:'#6366f1'}
    ];
    var ph='';
    metrics.forEach(function(m){
      var w=Math.min((m.p||0)/60*100,100);
      ph+='<div class="metric-row"><span class="metric-name">'+m.name+'</span>';
      ph+='<div class="pct-bar-wrap" style="flex:1;margin:0 16px"><div class="pct-bar" style="width:'+w+'%;background:'+m.color+'"></div></div>';
      ph+='<span class="pct-label">'+pct(m.p)+'</span>';
      ph+='<span class="metric-val" style="min-width:90px;text-align:right">'+fmt(m.val)+'</span></div>';
    });
    // Net profit section
    ph+='<div style="border-top:2px solid #e5e7eb;margin-top:12px;padding-top:12px">';
    ph+='<div class="metric-row"><span class="metric-name">Adjusted Revenue</span><span class="metric-val">'+fmtD(rev.adjusted_net_revenue)+'</span></div>';
    ph+='<div class="metric-row"><span class="metric-name">Less: Adjusted Expenses</span><span class="metric-val">('+fmtD(exp.total_expenses_adjusted)+')</span></div>';
    ph+='<div class="metric-row"><span class="metric-name" style="font-weight:700">Net Profit (Bank Only)</span><span class="metric-val" style="color:'+(prof.net_profit_bank_only>=0?'#27ae60':'#e74c3c')+'">'+fmtD(prof.net_profit_bank_only)+' <span style="font-size:0.8rem;opacity:0.7">'+pct(prof.margin_pct_bank_only)+'</span></span></div>';
    ph+='</div>';
    // Cash reconciliation
    ph+='<div style="border-top:2px solid #e5e7eb;margin-top:12px;padding-top:12px">';
    ph+='<div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:0.5px;color:#666;margin-bottom:8px;font-weight:600">Cash Reconciliation</div>';
    ph+='<div class="metric-row"><span class="metric-name">Toast Cash Collected</span><span class="metric-val">'+fmtD(cash.toast_cash_collected)+'</span></div>';
    ph+='<div class="metric-row"><span class="metric-name">Bank Cash Deposited</span><span class="metric-val">'+fmtD(cash.bank_cash_deposited)+'</span></div>';
    var undeposited=cash.undeposited_cash||0;
    ph+='<div class="metric-row"><span class="metric-name">Undeposited Cash</span><span class="metric-val" style="color:'+(undeposited>0?'#e74c3c':'#27ae60')+'">'+fmtD(undeposited)+'</span></div>';
    ph+='<div class="metric-row" style="margin-top:4px"><span class="metric-name">Net Profit (Bank Only)</span><span class="metric-val">'+fmtD(prof.net_profit_bank_only)+'</span></div>';
    ph+='<div class="metric-row"><span class="metric-name">+ Undeposited Cash</span><span class="metric-val">'+fmtD(undeposited)+'</span></div>';
    ph+='<div class="metric-row"><span class="metric-name" style="font-weight:700">Net Profit (Cash Adjusted)</span><span class="metric-val" style="color:'+(prof.net_profit_cash_adjusted>=0?'#27ae60':'#e74c3c')+'">'+fmtD(prof.net_profit_cash_adjusted)+' <span style="font-size:0.8rem;opacity:0.7">'+pct(prof.margin_pct_cash_adjusted)+'</span></span></div>';
    ph+='</div>';
    $('profitBody').innerHTML=ph;
  }

  // Auto-load on page open
  loadReport();
})();
</script>
</body>
</html>'''



def _analysis_dashboard_html() -> str:
    """Return self-contained HTML for the comprehensive analysis dashboard."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Comprehensive Analysis</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}
.header{background:linear-gradient(135deg,#0f0c29,#302b63,#24243e);color:#fff;padding:24px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.header h1{font-size:1.5rem;font-weight:700;letter-spacing:0.5px}
.header .subtitle{font-size:0.85rem;opacity:0.7}
.nav-bar{background:#fff;border-bottom:1px solid #e5e7eb;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#374151;transition:all 0.15s}
.nav-bar a:hover{background:#f3f4f6}
.nav-bar a.active{background:#6366f1;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.kpi-card .label{font-size:0.8rem;text-transform:uppercase;letter-spacing:0.5px;color:#666;margin-bottom:4px}
.kpi-card .value{font-size:1.8rem;font-weight:700}
.kpi-card .value.ok{color:#27ae60}
.kpi-card .value.warn{color:#e74c3c}
.kpi-card .value.info{color:#2980b9}
.filter-bar{background:#fff;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.filter-bar .field{display:flex;flex-direction:column;gap:4px}
.filter-bar .field label{font-size:0.75rem;font-weight:600;color:#555;text-transform:uppercase}
.filter-bar input{padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:0.875rem;background:#fff}
.filter-bar input:focus{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,0.1)}
.filter-bar button{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.btn-primary{background:#6366f1;color:#fff}.btn-primary:hover{background:#4f46e5}
.section{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;overflow:hidden}
.section-header{padding:16px 20px;border-bottom:1px solid #e5e7eb;font-size:1rem;font-weight:700;color:#1a1a2e}
.section-body{padding:20px;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
thead{background:#f8f9fa}
th{padding:10px 14px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;white-space:nowrap;cursor:pointer;user-select:none}
th:hover{background:#eef0f4}
th .sort-arrow{font-size:0.7rem;margin-left:4px;opacity:0.4}
th.sorted .sort-arrow{opacity:1}
td{padding:10px 14px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
tr:hover{background:#f8f9ff}
.amount{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
tfoot td{font-weight:700;border-top:2px solid #e5e7eb}
.bar-cell{display:flex;align-items:center;gap:10px}
.bar-track{flex:1;height:22px;background:#f0f0f0;border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;min-width:2px}
.bar-fill.indigo{background:linear-gradient(90deg,#6366f1,#818cf8)}
.bar-fill.green{background:linear-gradient(90deg,#10b981,#34d399)}
.bar-value{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;font-size:0.8rem;font-weight:600;min-width:70px;text-align:right}
details.assumptions{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;overflow:hidden}
details.assumptions summary{padding:16px 20px;font-size:0.9rem;font-weight:700;cursor:pointer;color:#1a1a2e}
details.assumptions .body{padding:0 20px 16px;font-size:0.82rem;color:#555;line-height:1.6}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.2);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#fff;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.15);text-align:center}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #6366f1;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
.toast-container{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px}
.toast{padding:12px 20px;border-radius:8px;color:#fff;font-size:0.875rem;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.15);animation:slideIn 0.3s ease}
.toast.success{background:#10b981}.toast.error{background:#ef4444}.toast.info{background:#6366f1}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
@media(max-width:768px){.container{padding:12px}.header{padding:16px}.kpi-row{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>LOV3 Comprehensive Analysis</h1>
    <div class="subtitle">Monthly P&amp;L, revenue by day-of-week, and hourly revenue profile</div>
  </div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis" class="active">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <div class="field">
      <label>Start Date</label>
      <input type="date" id="startDate">
    </div>
    <div class="field">
      <label>End Date</label>
      <input type="date" id="endDate">
    </div>
    <button class="btn-primary" onclick="loadAnalysis()">Load Analysis</button>
  </div>

  <div id="reportContent" class="hidden">

    <div class="kpi-row" id="kpiRow">
      <div class="kpi-card"><div class="label">Adjusted Revenue</div><div class="value info" id="kpiRevenue">--</div></div>
      <div class="kpi-card"><div class="label">Net Profit</div><div class="value ok" id="kpiProfit">--</div></div>
      <div class="kpi-card"><div class="label">Margin %</div><div class="value info" id="kpiMargin">--</div></div>
      <div class="kpi-card"><div class="label">Prime Cost %</div><div class="value warn" id="kpiPrime">--</div></div>
      <div class="kpi-card"><div class="label">Months</div><div class="value info" id="kpiMonths">--</div></div>
    </div>

    <!-- Assumptions -->
    <details class="assumptions" id="assumptionsSection">
      <summary>Business Assumptions</summary>
      <div class="body" id="assumptionsBody"></div>
    </details>

    <!-- Monthly P&L -->
    <div class="section" id="monthlySection">
      <div class="section-header">Monthly P&amp;L</div>
      <div class="section-body" id="monthlyBody"></div>
    </div>

    <!-- Revenue by Day-of-Week -->
    <div class="section" id="dowSection">
      <div class="section-header">Revenue by Day of Week</div>
      <div class="section-body" id="dowBody"></div>
    </div>

    <!-- Hourly Revenue Profile -->
    <div class="section" id="hourlySection">
      <div class="section-header">Hourly Revenue Profile</div>
      <div class="section-body" id="hourlyBody"></div>
    </div>

  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Loading analysis&hellip;</div>
</div>
<div class="toast-container" id="toastContainer"></div>

<script>
(function(){
  var $ = function(id){return document.getElementById(id)};

  function fmt(n){
    if(n==null) return '--';
    return n.toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:0,maximumFractionDigits:0});
  }
  function fmtD(n){
    if(n==null) return '--';
    return n.toLocaleString('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:2});
  }
  function pct(n){return n!=null?(n.toFixed(1)+'%'):'--'}
  function showToast(msg,type){
    var c=$('toastContainer'),d=document.createElement('div');
    d.className='toast '+(type||'info');d.textContent=msg;c.appendChild(d);
    setTimeout(function(){d.remove()},4000);
  }
  function fmtHour(h){
    if(h===0) return '12 AM';
    if(h<12) return h+' AM';
    if(h===12) return '12 PM';
    return (h-12)+' PM';
  }

  // Default dates: 3 months ago -> today
  var today=new Date();
  var y=today.getFullYear(),m=today.getMonth();
  var three=new Date(y,m-3,1);
  $('startDate').value=three.getFullYear()+'-'+String(three.getMonth()+1).padStart(2,'0')+'-01';
  $('endDate').value=today.toISOString().slice(0,10);

  var monthlyData=[];
  var sortCol='month';
  var sortAsc=true;

  window.loadAnalysis=function(){
    var sd=$('startDate').value, ed=$('endDate').value;
    if(!sd||!ed){showToast('Please select both dates','error');return;}
    $('loadingOverlay').classList.add('active');
    $('reportContent').classList.add('hidden');

    fetch('/comprehensive-analysis',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({start_date:sd,end_date:ed})
    })
    .then(function(r){
      if(!r.ok) return r.json().then(function(e){throw new Error(e.error||'Request failed')});
      return r.json();
    })
    .then(function(d){
      renderAnalysis(d);
      $('reportContent').classList.remove('hidden');
    })
    .catch(function(e){showToast(e.message,'error')})
    .finally(function(){$('loadingOverlay').classList.remove('active')});
  };

  function renderAnalysis(d){
    var s=d.summary_pnl||{}, a=d.assumptions||{};

    // KPIs
    $('kpiRevenue').textContent=fmt(s.adjusted_revenue);
    var np=s.net_profit;
    $('kpiProfit').textContent=fmt(np);
    $('kpiProfit').className='value '+(np>=0?'ok':'warn');
    $('kpiMargin').textContent=pct(s.margin_pct);
    $('kpiPrime').textContent=pct(s.prime_cost_pct);
    $('kpiMonths').textContent=d.num_months!=null?d.num_months:'--';

    // Assumptions
    var notes=a.notes||[];
    var ah='<ul>';
    notes.forEach(function(n){ah+='<li>'+n+'</li>'});
    ah+='</ul>';
    ah+='<p style="margin-top:8px"><strong>Business day cutoff:</strong> '+(a.business_day_cutoff_hour||4)+':00 AM</p>';
    ah+='<p><strong>Gratuity retained:</strong> '+((a.gratuity_retain_pct||0.35)*100).toFixed(0)+'%</p>';
    $('assumptionsBody').innerHTML=ah;

    // Monthly P&L (sortable)
    monthlyData=d.monthly_pnl||[];
    sortCol='month';sortAsc=true;
    renderMonthlyTable();

    // Revenue by day of week
    var dow=d.revenue_by_business_day||[];
    var maxDow=0;
    dow.forEach(function(r){if(r.avg_daily_revenue>maxDow)maxDow=r.avg_daily_revenue});
    var dh='<table><thead><tr><th>Day</th><th>Avg Daily Rev</th><th style="width:35%">Distribution</th><th class="text-right">Gross Revenue</th><th class="text-right">Avg Check</th><th class="text-right">Days</th><th class="text-right">Txns</th></tr></thead><tbody>';
    dow.forEach(function(r){
      var w=maxDow>0?(r.avg_daily_revenue/maxDow*100):0;
      dh+='<tr><td><strong>'+r.day+'</strong></td>';
      dh+='<td class="amount">'+fmt(r.avg_daily_revenue)+'</td>';
      dh+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill indigo" style="width:'+w.toFixed(1)+'%"></div></div></div></td>';
      dh+='<td class="amount">'+fmt(r.gross_revenue)+'</td>';
      dh+='<td class="amount">'+fmtD(r.avg_check)+'</td>';
      dh+='<td class="amount">'+(r.num_days||0)+'</td>';
      dh+='<td class="amount">'+(r.txn_count||0).toLocaleString()+'</td></tr>';
    });
    dh+='</tbody></table>';
    $('dowBody').innerHTML=dh;

    // Hourly revenue profile
    var hourly=d.hourly_revenue_profile||[];
    var maxH=0;
    hourly.forEach(function(r){if(r.avg_daily_revenue>maxH)maxH=r.avg_daily_revenue});
    var hh='<table><thead><tr><th>Hour</th><th>Avg Daily Rev</th><th style="width:40%">Distribution</th><th class="text-right">Gross Revenue</th><th class="text-right">Avg Check</th><th class="text-right">Txns</th></tr></thead><tbody>';
    hourly.forEach(function(r){
      var w=maxH>0?(r.avg_daily_revenue/maxH*100):0;
      hh+='<tr><td><strong>'+fmtHour(r.hour)+'</strong></td>';
      hh+='<td class="amount">'+fmt(r.avg_daily_revenue)+'</td>';
      hh+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill green" style="width:'+w.toFixed(1)+'%"></div></div></div></td>';
      hh+='<td class="amount">'+fmt(r.gross_revenue)+'</td>';
      hh+='<td class="amount">'+fmtD(r.avg_check)+'</td>';
      hh+='<td class="amount">'+(r.txn_count||0).toLocaleString()+'</td></tr>';
    });
    hh+='</tbody></table>';
    $('hourlyBody').innerHTML=hh;
  }

  function renderMonthlyTable(){
    var data=monthlyData.slice().sort(function(a,b){
      var va=a[sortCol],vb=b[sortCol];
      if(va==null) va=sortCol==='month'?'':0;
      if(vb==null) vb=sortCol==='month'?'':0;
      if(typeof va==='string') return sortAsc?va.localeCompare(vb):vb.localeCompare(va);
      return sortAsc?(va-vb):(vb-va);
    });

    var cols=[
      {key:'month',label:'Month',fmt:function(v){return v||'--'}},
      {key:'adjusted_revenue',label:'Revenue',fmt:fmt},
      {key:'cogs',label:'COGS',fmt:fmt},
      {key:'cogs_pct',label:'COGS%',fmt:function(v,row){var r=row.adjusted_revenue||1;return pct(row.cogs/r*100)}},
      {key:'labor_true',label:'Labor',fmt:fmt},
      {key:'labor_pct',label:'Labor%',fmt:function(v,row){var r=row.adjusted_revenue||1;return pct(row.labor_true/r*100)}},
      {key:'marketing',label:'Marketing',fmt:fmt},
      {key:'opex',label:'OPEX',fmt:fmt},
      {key:'total_expenses_adjusted',label:'Expenses',fmt:fmt},
      {key:'net_profit',label:'Net Profit',fmt:fmt},
      {key:'margin_pct',label:'Margin%',fmt:function(v,row){var r=row.adjusted_revenue||1;return pct(row.net_profit/r*100)}}
    ];

    var th='<table><thead><tr>';
    cols.forEach(function(c){
      var arrow=sortCol===c.key?(sortAsc?' &#9650;':' &#9660;'):' &#9650;';
      var cls=sortCol===c.key?' class="sorted"':'';
      th+='<th'+cls+' data-col="'+c.key+'">'+c.label+'<span class="sort-arrow">'+arrow+'</span></th>';
    });
    th+='</tr></thead><tbody>';
    data.forEach(function(row){
      th+='<tr>';
      cols.forEach(function(c){
        var val=c.fmt(row[c.key],row);
        var cls=c.key!=='month'?' class="amount"':'';
        if(c.key==='net_profit'){
          var color=row.net_profit>=0?'#27ae60':'#e74c3c';
          cls=' class="amount" style="color:'+color+'"';
        }
        th+='<td'+cls+'>'+val+'</td>';
      });
      th+='</tr>';
    });
    th+='</tbody></table>';
    $('monthlyBody').innerHTML=th;

    // Attach sort listeners
    var headers=$('monthlyBody').querySelectorAll('th[data-col]');
    headers.forEach(function(h){
      h.addEventListener('click',function(){
        var col=this.getAttribute('data-col');
        if(sortCol===col){sortAsc=!sortAsc}else{sortCol=col;sortAsc=col==='month'}
        renderMonthlyTable();
      });
    });
  }

  // Auto-load on page open
  loadAnalysis();
})();
</script>
</body>
</html>'''



def _cash_recon_html() -> str:
    """Return self-contained HTML for the cash reconciliation dashboard."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Cash Reconciliation</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f3f4f6;color:#1a1a2e;min-height:100vh}
.header{background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 50%,#a78bfa 100%);color:#fff;padding:20px 24px}
.header h1{font-size:1.5rem;font-weight:700}.header .subtitle{font-size:0.85rem;opacity:0.85;margin-top:4px}
.nav-bar{display:flex;gap:0;background:#1a1a2e;padding:0 16px;flex-wrap:wrap}
.nav-bar a{color:#94a3b8;text-decoration:none;padding:12px 16px;font-size:0.82rem;font-weight:500;transition:all 0.15s;border-bottom:2px solid transparent;white-space:nowrap}
.nav-bar a:hover{color:#fff;background:rgba(255,255,255,0.05)}
.nav-bar a.active{color:#fff;border-bottom-color:#6366f1;background:rgba(99,102,241,0.1)}
.container{max-width:1400px;margin:0 auto;padding:20px}
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.kpi-card .label{font-size:0.75rem;font-weight:600;text-transform:uppercase;letter-spacing:0.05em;color:#6b7280;margin-bottom:8px}
.kpi-card .value{font-size:1.5rem;font-weight:700;font-family:"SF Mono",SFMono-Regular,Menlo,monospace}
.kpi-card .sub{font-size:0.75rem;color:#6b7280;margin-top:4px}
.kpi-card .value.good{color:#10b981}.kpi-card .value.warn{color:#f59e0b}.kpi-card .value.bad{color:#ef4444}
.filter-bar{display:flex;gap:12px;align-items:center;margin-bottom:24px;flex-wrap:wrap}
.filter-bar label{font-size:0.8rem;font-weight:600;color:#374151}
.filter-bar input{padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:0.875rem;background:#fff}
.filter-bar input:focus{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,0.1)}
.filter-bar button{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.btn-primary{background:#6366f1;color:#fff}.btn-primary:hover{background:#4f46e5}
.section{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;overflow:hidden}
.section-header{padding:16px 20px;border-bottom:1px solid #e5e7eb;font-size:1rem;font-weight:700;color:#1a1a2e}
.section-body{padding:20px;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
thead{background:#f8f9fa}
th{padding:10px 14px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
tr:hover{background:#f8f9ff}
.amount{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
tfoot td{font-weight:700;border-top:2px solid #e5e7eb}
.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:0.75rem;font-weight:600}
.badge-ok{background:#d1fae5;color:#065f46}
.badge-watch{background:#fef3c7;color:#92400e}
.badge-high{background:#fee2e2;color:#991b1b}
.flag-icon{color:#ef4444;font-weight:700}
.alert-banner{background:#fee2e2;border:1px solid #ef4444;border-radius:12px;padding:16px 20px;margin-bottom:24px;font-size:0.85rem;color:#991b1b}
.alert-banner ul{margin:8px 0 0 20px}.alert-banner li{margin:4px 0}
.alert-banner.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.2);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#fff;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.15);text-align:center}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #6366f1;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
.toast-container{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px}
.toast{padding:12px 20px;border-radius:8px;color:#fff;font-size:0.875rem;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.15);animation:slideIn 0.3s ease}
.toast.success{background:#10b981}.toast.error{background:#ef4444}.toast.info{background:#6366f1}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
.highlight-row{background:#fef3c7 !important}
@media(max-width:768px){.container{padding:12px}.header{padding:16px}.kpi-row{grid-template-columns:1fr 1fr}.filter-bar{flex-direction:column;align-items:stretch}.nav-bar{padding:0 8px}.nav-bar a{padding:10px 12px;font-size:0.75rem}}
@media(max-width:480px){.kpi-row{grid-template-columns:1fr}.kpi-card{padding:14px}.kpi-card .value{font-size:1.2rem}.header h1{font-size:1.2rem}}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>LOV3 Cash Reconciliation</h1>
    <div class="subtitle">POS collections vs bank deposits &mdash; credit card settlement &amp; cash tracking</div>
  </div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon" class="active">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label>
    <input type="date" id="startDate">
    <label>To</label>
    <input type="date" id="endDate">
    <button class="btn-primary" onclick="loadRecon()">Load</button>
  </div>

  <div class="kpi-row">
    <div class="kpi-card"><div class="label">POS Collected (Net)</div><div class="value" id="kpiPosNet">--</div><div class="sub" id="kpiPosSub"></div></div>
    <div class="kpi-card"><div class="label">Bank Deposited</div><div class="value" id="kpiBankNet">--</div><div class="sub" id="kpiBankSub"></div></div>
    <div class="kpi-card"><div class="label">Card Recon %</div><div class="value" id="kpiCardPct">--</div><div class="sub" id="kpiCardSub"></div></div>
    <div class="kpi-card"><div class="label">Undeposited Cash</div><div class="value" id="kpiCashGap">--</div><div class="sub" id="kpiCashSub"></div></div>
  </div>

  <div class="alert-banner hidden" id="alertBanner">
    <strong>Alerts</strong>
    <ul id="alertList"></ul>
  </div>

  <div class="section">
    <div class="section-header">Credit Card Reconciliation</div>
    <div class="section-body"><table id="cardTable"><thead><tr>
      <th>Month</th><th class="amount">POS Credit Net</th><th class="amount">Bank Card Net</th>
      <th class="amount">Difference</th><th class="amount">Cum. Diff</th><th>Status</th>
    </tr></thead><tbody></tbody><tfoot></tfoot></table></div>
  </div>

  <div class="section">
    <div class="section-header">Cash Reconciliation</div>
    <div class="section-body"><table id="cashTable"><thead><tr>
      <th>Month</th><th class="amount">POS Cash</th><th class="amount">Counter Credit</th>
      <th class="amount">Cash Acct (9121)</th><th class="amount">Total Cash In</th>
      <th class="amount">Gap</th><th class="amount">Cum. Gap</th><th></th>
    </tr></thead><tbody></tbody><tfoot></tfoot></table></div>
  </div>

  <div class="section">
    <div class="section-header">Bank Deposit Breakdown</div>
    <div class="section-body"><table id="depositTable"><thead><tr>
      <th>Month</th><th class="amount">Citizens Settle</th><th class="amount">Toast DEP</th>
      <th class="amount">Toast EOM</th><th class="amount">Platform Fee</th>
      <th class="amount">Total Card</th><th class="amount">Counter Credit</th>
      <th class="amount">Cash Acct (9121)</th>
    </tr></thead><tbody></tbody><tfoot></tfoot></table></div>
  </div>

  <div class="section">
    <div class="section-header">POS Status Breakdown</div>
    <div class="section-body"><table id="statusTable"><thead><tr>
      <th>Month</th>
      <th class="amount">CAPTURED #</th><th class="amount">CAPTURED $</th>
      <th class="amount">AUTHORIZED #</th><th class="amount">AUTHORIZED $</th>
      <th class="amount">CAP_IN_PROG #</th><th class="amount">CAP_IN_PROG $</th>
    </tr></thead><tbody></tbody></table></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Loading reconciliation data&hellip;</div>
</div>
<div class="toast-container" id="toastContainer"></div>

<script>
(function(){
  // Default date range: 6 months back -> today
  const today = new Date();
  const sixAgo = new Date(today);
  sixAgo.setMonth(sixAgo.getMonth() - 6);
  sixAgo.setDate(1);
  document.getElementById('startDate').value = sixAgo.toISOString().slice(0,10);
  document.getElementById('endDate').value = today.toISOString().slice(0,10);

  function fmt(v){
    if(v==null) return '--';
    const n=Number(v);
    const s=Math.abs(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
    return n<0 ? '-$'+s : '$'+s;
  }
  function pct(v){
    if(v==null) return '--';
    return Number(v).toFixed(1)+'%';
  }
  function showToast(msg,type){
    const c=document.getElementById('toastContainer');
    const t=document.createElement('div');t.className='toast '+type;t.textContent=msg;
    c.appendChild(t);setTimeout(()=>t.remove(),4000);
  }
  function statusBadge(status){
    if(!status) return '';
    const cls = status==='OK'?'badge-ok':status==='WATCH'?'badge-watch':'badge-high';
    return '<span class="badge '+cls+'">'+status+'</span>';
  }
  function diffColor(v){
    const n=Number(v);
    if(n>0) return 'color:#10b981';
    if(n<0) return 'color:#ef4444';
    return '';
  }

  window.loadRecon = function(){
    const start = document.getElementById('startDate').value;
    const end = document.getElementById('endDate').value;
    if(!start||!end){showToast('Select date range','error');return;}
    const overlay = document.getElementById('loadingOverlay');
    overlay.classList.add('active');

    fetch('/api/cash-recon',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({start_date:start,end_date:end})
    })
    .then(r=>{if(!r.ok) throw new Error('API error '+r.status);return r.json();})
    .then(data=>{
      overlay.classList.remove('active');
      renderKPIs(data.totals);
      renderAlerts(data.alerts);
      renderCardTable(data.months, data.totals);
      renderCashTable(data.months, data.totals);
      renderDepositTable(data.months, data.totals);
      renderStatusTable(data.months);
    })
    .catch(e=>{
      overlay.classList.remove('active');
      showToast(e.message,'error');
    });
  };

  function renderKPIs(t){
    if(!t){return;}
    document.getElementById('kpiPosNet').textContent = fmt(t.pos_credit_net + t.pos_cash);
    document.getElementById('kpiPosSub').textContent = 'Credit: '+fmt(t.pos_credit_net)+' | Cash: '+fmt(t.pos_cash);
    document.getElementById('kpiBankNet').textContent = fmt(t.bank_card_net + t.bank_cash);
    document.getElementById('kpiBankSub').textContent = 'Card: '+fmt(t.bank_card_net)+' | Cash+9121: '+fmt(t.bank_cash);
    const cardEl = document.getElementById('kpiCardPct');
    cardEl.textContent = pct(t.card_recon_pct);
    cardEl.className = 'value '+(t.card_recon_pct>=95?'good':t.card_recon_pct>=90?'warn':'bad');
    document.getElementById('kpiCardSub').textContent = 'Diff: '+fmt(t.total_card_diff);
    const cashEl = document.getElementById('kpiCashGap');
    cashEl.textContent = fmt(t.undeposited_cash);
    cashEl.className = 'value '+(t.cash_deposited_pct>=80?'good':t.cash_deposited_pct>=50?'warn':'bad');
    document.getElementById('kpiCashSub').textContent = pct(t.cash_deposited_pct)+' deposited';
  }

  function renderAlerts(alerts){
    const banner = document.getElementById('alertBanner');
    const list = document.getElementById('alertList');
    list.innerHTML = '';
    if(!alerts||!alerts.length){banner.classList.add('hidden');return;}
    banner.classList.remove('hidden');
    alerts.forEach(a=>{
      const li = document.createElement('li');
      li.textContent = a.month+': '+a.message;
      list.appendChild(li);
    });
  }

  function renderCardTable(months, totals){
    const tbody = document.querySelector('#cardTable tbody');
    const tfoot = document.querySelector('#cardTable tfoot');
    tbody.innerHTML='';tfoot.innerHTML='';
    if(!months||!months.length) return;
    months.forEach(m=>{
      const r = m.recon;
      tbody.innerHTML += '<tr>'
        +'<td>'+m.month+'</td>'
        +'<td class="amount">'+fmt(m.pos.credit_net)+'</td>'
        +'<td class="amount">'+fmt(m.bank.net_card)+'</td>'
        +'<td class="amount" style="'+diffColor(r.card_diff)+'">'+fmt(r.card_diff)+'</td>'
        +'<td class="amount" style="'+diffColor(r.card_cum_diff)+'">'+fmt(r.card_cum_diff)+'</td>'
        +'<td>'+statusBadge(r.card_status)+'</td>'
        +'</tr>';
    });
    if(totals){
      tfoot.innerHTML = '<tr><td>Total</td>'
        +'<td class="amount">'+fmt(totals.pos_credit_net)+'</td>'
        +'<td class="amount">'+fmt(totals.bank_card_net)+'</td>'
        +'<td class="amount" style="'+diffColor(totals.total_card_diff)+'">'+fmt(totals.total_card_diff)+'</td>'
        +'<td></td><td>'+statusBadge(Math.abs(totals.card_recon_pct-100)<5?'OK':Math.abs(totals.card_recon_pct-100)<10?'WATCH':'HIGH')+'</td></tr>';
    }
  }

  function renderCashTable(months, totals){
    const tbody = document.querySelector('#cashTable tbody');
    const tfoot = document.querySelector('#cashTable tfoot');
    tbody.innerHTML='';tfoot.innerHTML='';
    if(!months||!months.length) return;
    months.forEach(m=>{
      const flag = m.bank.total_cash_in===0 ? '<span class="flag-icon">&#9888;</span>' : '';
      tbody.innerHTML += '<tr class="'+(m.bank.total_cash_in===0&&m.pos.cash_collected>0?'highlight-row':'')+'">'
        +'<td>'+m.month+'</td>'
        +'<td class="amount">'+fmt(m.pos.cash_collected)+'</td>'
        +'<td class="amount">'+fmt(m.bank.counter_credit)+'</td>'
        +'<td class="amount">'+fmt(m.bank.interaccount_in)+'</td>'
        +'<td class="amount" style="font-weight:700">'+fmt(m.bank.total_cash_in)+'</td>'
        +'<td class="amount bad">'+fmt(m.recon.cash_gap)+'</td>'
        +'<td class="amount bad">'+fmt(m.recon.cash_cum_gap)+'</td>'
        +'<td>'+flag+'</td>'
        +'</tr>';
    });
    if(totals){
      tfoot.innerHTML = '<tr><td>Total</td>'
        +'<td class="amount">'+fmt(totals.pos_cash)+'</td>'
        +'<td class="amount" colspan="2"></td>'
        +'<td class="amount" style="font-weight:700">'+fmt(totals.bank_cash)+'</td>'
        +'<td class="amount bad">'+fmt(totals.undeposited_cash)+'</td>'
        +'<td></td><td></td></tr>';
    }
  }

  function renderDepositTable(months, totals){
    const tbody = document.querySelector('#depositTable tbody');
    const tfoot = document.querySelector('#depositTable tfoot');
    tbody.innerHTML='';tfoot.innerHTML='';
    if(!months||!months.length) return;
    let tCitizens=0,tDep=0,tEom=0,tFee=0,tCard=0,tCash=0,tXfer=0;
    months.forEach(m=>{
      const b=m.bank;
      tCitizens+=b.citizens_settlement;tDep+=b.toast_dep;tEom+=b.toast_eom;
      tFee+=b.platform_fee;tCard+=b.total_card_deposits;tCash+=b.counter_credit;
      tXfer+=b.interaccount_in;
      tbody.innerHTML += '<tr>'
        +'<td>'+m.month+'</td>'
        +'<td class="amount">'+fmt(b.citizens_settlement)+'</td>'
        +'<td class="amount">'+fmt(b.toast_dep)+'</td>'
        +'<td class="amount">'+fmt(b.toast_eom)+'</td>'
        +'<td class="amount" style="color:#ef4444">'+fmt(b.platform_fee)+'</td>'
        +'<td class="amount" style="font-weight:700">'+fmt(b.total_card_deposits)+'</td>'
        +'<td class="amount">'+fmt(b.counter_credit)+'</td>'
        +'<td class="amount">'+fmt(b.interaccount_in)+'</td>'
        +'</tr>';
    });
    tfoot.innerHTML = '<tr><td>Total</td>'
      +'<td class="amount">'+fmt(tCitizens)+'</td>'
      +'<td class="amount">'+fmt(tDep)+'</td>'
      +'<td class="amount">'+fmt(tEom)+'</td>'
      +'<td class="amount" style="color:#ef4444">'+fmt(tFee)+'</td>'
      +'<td class="amount" style="font-weight:700">'+fmt(tCard)+'</td>'
      +'<td class="amount">'+fmt(tCash)+'</td>'
      +'<td class="amount">'+fmt(tXfer)+'</td>'
      +'</tr>';
  }

  function renderStatusTable(months){
    const tbody = document.querySelector('#statusTable tbody');
    tbody.innerHTML='';
    if(!months||!months.length) return;
    months.forEach(m=>{
      const sb = m.pos.status_breakdown||{};
      const cap = sb['CAPTURED']||{count:0,amount:0};
      const auth = sb['AUTHORIZED']||{count:0,amount:0};
      const cip = sb['CAPTURE_IN_PROGRESS']||{count:0,amount:0};
      const hlClass = auth.count > 100 ? ' highlight-row' : '';
      tbody.innerHTML += '<tr class="'+hlClass+'">'
        +'<td>'+m.month+'</td>'
        +'<td class="amount">'+cap.count.toLocaleString()+'</td>'
        +'<td class="amount">'+fmt(cap.amount)+'</td>'
        +'<td class="amount'+(auth.count>100?' bad':'')+'">'+auth.count.toLocaleString()+'</td>'
        +'<td class="amount">'+fmt(auth.amount)+'</td>'
        +'<td class="amount">'+cip.count.toLocaleString()+'</td>'
        +'<td class="amount">'+fmt(cip.amount)+'</td>'
        +'</tr>';
    });
  }

  // Auto-load on page open
  loadRecon();
})();
</script>
</body>
</html>'''



def _menu_mix_html() -> str:
    """Return self-contained HTML for the menu mix analysis dashboard."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Menu Mix Analysis</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}
.header{background:linear-gradient(135deg,#4c1d95,#7c3aed,#6d28d9);color:#fff;padding:24px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.header h1{font-size:1.5rem;font-weight:700;letter-spacing:0.5px}
.header .subtitle{font-size:0.85rem;opacity:0.7}
.nav-bar{background:#fff;border-bottom:1px solid #e5e7eb;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#374151;transition:all 0.15s}
.nav-bar a:hover{background:#f3f4f6}
.nav-bar a.active{background:#7c3aed;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,0.08)}
.kpi-card .label{font-size:0.8rem;text-transform:uppercase;letter-spacing:0.5px;color:#666;margin-bottom:4px}
.kpi-card .value{font-size:1.8rem;font-weight:700}
.kpi-card .value.ok{color:#27ae60}
.kpi-card .value.warn{color:#f59e0b}
.kpi-card .value.danger{color:#e74c3c}
.kpi-card .value.info{color:#7c3aed}
.filter-bar{background:#fff;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.filter-bar .field{display:flex;flex-direction:column;gap:4px}
.filter-bar .field label{font-size:0.75rem;font-weight:600;color:#555;text-transform:uppercase}
.filter-bar input{padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:0.875rem;background:#fff}
.filter-bar input:focus{outline:none;border-color:#7c3aed;box-shadow:0 0 0 3px rgba(124,58,237,0.1)}
.filter-bar button{padding:8px 20px;border:none;border-radius:8px;font-size:0.875rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.btn-primary{background:#7c3aed;color:#fff}.btn-primary:hover{background:#6d28d9}
.section{background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.08);margin-bottom:24px;overflow:hidden}
.section-header{padding:16px 20px;border-bottom:1px solid #e5e7eb;font-size:1rem;font-weight:700;color:#1a1a2e}
.section-body{padding:20px;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
thead{background:#f8f9fa}
th{padding:10px 14px;text-align:left;font-weight:600;color:#374151;border-bottom:2px solid #e5e7eb;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #f0f0f0;vertical-align:middle}
tr:hover{background:#f8f9ff}
.amount{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
tfoot td{font-weight:700;border-top:2px solid #e5e7eb}
.bar-cell{display:flex;align-items:center;gap:10px}
.bar-track{flex:1;height:22px;background:#f0f0f0;border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;min-width:2px}
.bar-fill.purple{background:linear-gradient(90deg,#7c3aed,#a78bfa)}
.bar-fill.indigo{background:linear-gradient(90deg,#6366f1,#818cf8)}
.bar-fill.green{background:linear-gradient(90deg,#10b981,#34d399)}
.bar-fill.amber{background:linear-gradient(90deg,#f59e0b,#fbbf24)}
.bar-value{font-family:"SF Mono",SFMono-Regular,Menlo,monospace;font-size:0.8rem;font-weight:600;min-width:70px;text-align:right}
.rank-num{font-weight:700;color:#7c3aed}
.peak-row{background:#f0fdf4!important}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.2);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#fff;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.15);text-align:center}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #7c3aed;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
.toast-container{position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px}
.toast{padding:12px 20px;border-radius:8px;color:#fff;font-size:0.875rem;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.15);animation:slideIn 0.3s ease}
.toast.success{background:#10b981}.toast.error{background:#ef4444}.toast.info{background:#7c3aed}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
@media(max-width:768px){.container{padding:12px}.header{padding:16px}.kpi-row{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>LOV3 Menu Mix Analysis</h1>
    <div class="subtitle">Item performance, category breakdown, daypart &amp; day-of-week analysis</div>
  </div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix" class="active">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <div class="field"><label>Start Date</label><input type="date" id="startDate"></div>
    <div class="field"><label>End Date</label><input type="date" id="endDate"></div>
    <button class="btn-primary" onclick="loadMenuMix()">Analyze</button>
  </div>

  <div id="kpiRow" class="kpi-row"></div>

  <div class="section">
    <div class="section-header">Top 20 Items by Revenue</div>
    <div class="section-body" id="topItemsBody"><p style="color:#999;text-align:center;padding:40px">Select a date range and click Analyze</p></div>
  </div>

  <div class="section">
    <div class="section-header">Sales Category Breakdown</div>
    <div class="section-body" id="categoryBody"></div>
  </div>

  <div class="section">
    <div class="section-header">Service Period (Daypart) Performance</div>
    <div class="section-body" id="serviceBody"></div>
  </div>

  <div class="section">
    <div class="section-header">Day-of-Week Performance</div>
    <div class="section-body" id="dowBody"></div>
  </div>

  <div class="section">
    <div class="section-header">Hourly Revenue Profile</div>
    <div class="section-body" id="hourlyBody"></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Loading menu mix data&hellip;</div>
</div>
<div class="toast-container" id="toastContainer"></div>

<script>
(function(){
  var $=function(id){return document.getElementById(id)};
  var fmt=function(v){return v==null?'--':'$'+Number(v).toLocaleString(undefined,{minimumFractionDigits:0,maximumFractionDigits:0})};
  var fmtD=function(v){return v==null?'--':'$'+Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})};
  var pct=function(v){return v==null?'--':Number(v).toFixed(1)+'%'};
  var fmtHour=function(h){if(h===0)return '12 AM';if(h<12)return h+' AM';if(h===12)return '12 PM';return (h-12)+' PM'};

  // Default: 3 months back
  var now=new Date();
  var start=new Date(now);
  start.setMonth(start.getMonth()-3);
  $('startDate').value=start.toISOString().slice(0,10);
  $('endDate').value=now.toISOString().slice(0,10);

  function showToast(msg,type){
    var t=document.createElement('div');t.className='toast '+(type||'info');t.textContent=msg;
    $('toastContainer').appendChild(t);setTimeout(function(){t.remove()},4000);
  }

  function loadMenuMix(){
    var s=$('startDate').value,e=$('endDate').value;
    if(!s||!e){showToast('Select both dates','error');return}
    $('loadingOverlay').classList.add('active');

    fetch('/api/menu-mix',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:s,end_date:e})})
    .then(function(r){if(!r.ok)throw new Error('HTTP '+r.status);return r.json()})
    .then(function(d){
      $('loadingOverlay').classList.remove('active');
      if(d.error){showToast(d.error,'error');return}
      renderAll(d);
    })
    .catch(function(err){
      $('loadingOverlay').classList.remove('active');
      showToast('Failed: '+err.message,'error');
    });
  }

  function renderAll(d){
    var k=d.kpis||{};

    // KPI cards
    var voidCls=k.void_rate_pct<2?'ok':k.void_rate_pct<5?'warn':'danger';
    var kh='';
    kh+='<div class="kpi-card"><div class="label">Total Revenue</div><div class="value info">'+fmt(k.total_revenue)+'</div></div>';
    kh+='<div class="kpi-card"><div class="label">Items Sold</div><div class="value">'+(k.total_items_sold||0).toLocaleString()+'</div></div>';
    kh+='<div class="kpi-card"><div class="label">Unique Checks</div><div class="value">'+(k.unique_checks||0).toLocaleString()+'</div></div>';
    kh+='<div class="kpi-card"><div class="label">Avg Check Size</div><div class="value">'+fmtD(k.avg_check_size)+'</div></div>';
    kh+='<div class="kpi-card"><div class="label">Void Rate</div><div class="value '+voidCls+'">'+pct(k.void_rate_pct)+'</div></div>';
    $('kpiRow').innerHTML=kh;

    // Top 20 Items
    var items=d.top_items||[];
    var maxRev=0;items.forEach(function(r){if(r.net_revenue>maxRev)maxRev=r.net_revenue});
    var ih='<table><thead><tr><th>#</th><th>Item</th><th>Group</th><th>Menu</th><th class="text-right">Qty</th><th class="text-right">Revenue</th><th class="text-right">Avg Price</th><th style="width:20%">% of Total</th></tr></thead><tbody>';
    items.forEach(function(r,i){
      var w=maxRev>0?(r.net_revenue/maxRev*100):0;
      ih+='<tr><td class="rank-num">'+(i+1)+'</td>';
      ih+='<td><strong>'+r.menu_item+'</strong></td>';
      ih+='<td>'+r.menu_group+'</td>';
      ih+='<td>'+r.menu+'</td>';
      ih+='<td class="amount">'+(r.qty_sold||0).toLocaleString()+'</td>';
      ih+='<td class="amount">'+fmt(r.net_revenue)+'</td>';
      ih+='<td class="amount">'+fmtD(r.avg_price)+'</td>';
      ih+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill purple" style="width:'+w.toFixed(1)+'%"></div></div><div class="bar-value">'+pct(r.pct_of_total)+'</div></div></td></tr>';
    });
    ih+='</tbody></table>';
    $('topItemsBody').innerHTML=ih;

    // Category breakdown
    var cats=d.categories||[];
    var maxCat=0;cats.forEach(function(r){if(r.revenue>maxCat)maxCat=r.revenue});
    var ch='<table><thead><tr><th>Category</th><th class="text-right">Items</th><th class="text-right">Revenue</th><th style="width:30%">% of Total</th></tr></thead><tbody>';
    cats.forEach(function(r){
      var w=maxCat>0?(r.revenue/maxCat*100):0;
      ch+='<tr><td><strong>'+r.category+'</strong></td>';
      ch+='<td class="amount">'+(r.items||0).toLocaleString()+'</td>';
      ch+='<td class="amount">'+fmt(r.revenue)+'</td>';
      ch+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill indigo" style="width:'+w.toFixed(1)+'%"></div></div><div class="bar-value">'+pct(r.pct_of_total)+'</div></div></td></tr>';
    });
    ch+='</tbody></table>';
    $('categoryBody').innerHTML=ch;

    // Service periods
    var svc=d.service_periods||[];
    var maxSvc=0;svc.forEach(function(r){if(r.revenue>maxSvc)maxSvc=r.revenue});
    var sh='<table><thead><tr><th>Service</th><th class="text-right">Checks</th><th class="text-right">Revenue</th><th class="text-right">Avg Check</th><th style="width:25%">% Revenue</th></tr></thead><tbody>';
    svc.forEach(function(r){
      var w=maxSvc>0?(r.revenue/maxSvc*100):0;
      sh+='<tr><td><strong>'+r.service+'</strong></td>';
      sh+='<td class="amount">'+(r.checks||0).toLocaleString()+'</td>';
      sh+='<td class="amount">'+fmt(r.revenue)+'</td>';
      sh+='<td class="amount">'+fmtD(r.avg_check)+'</td>';
      sh+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill green" style="width:'+w.toFixed(1)+'%"></div></div><div class="bar-value">'+pct(r.pct_of_total)+'</div></div></td></tr>';
    });
    sh+='</tbody></table>';
    $('serviceBody').innerHTML=sh;

    // Day of week
    var dow=d.day_of_week||[];
    var maxDow=0;dow.forEach(function(r){if(r.revenue>maxDow)maxDow=r.revenue});
    // Find peak day
    var peakDay='';var peakRev=0;dow.forEach(function(r){if(r.revenue>peakRev){peakRev=r.revenue;peakDay=r.day}});
    var dh='<table><thead><tr><th>Day</th><th class="text-right">Checks</th><th class="text-right">Revenue</th><th class="text-right">Avg Check</th><th class="text-right">Avg Daily Rev</th><th style="width:25%">Distribution</th></tr></thead><tbody>';
    dow.forEach(function(r){
      var w=maxDow>0?(r.revenue/maxDow*100):0;
      var cls=r.day===peakDay?' class="peak-row"':'';
      dh+='<tr'+cls+'><td><strong>'+r.day+'</strong></td>';
      dh+='<td class="amount">'+(r.checks||0).toLocaleString()+'</td>';
      dh+='<td class="amount">'+fmt(r.revenue)+'</td>';
      dh+='<td class="amount">'+fmtD(r.avg_check)+'</td>';
      dh+='<td class="amount">'+fmt(r.avg_daily_revenue)+'</td>';
      dh+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill amber" style="width:'+w.toFixed(1)+'%"></div></div><div class="bar-value">'+pct(r.pct_of_total)+'</div></div></td></tr>';
    });
    dh+='</tbody></table>';
    $('dowBody').innerHTML=dh;

    // Hourly profile
    var hourly=d.hourly_profile||[];
    var maxH=0;hourly.forEach(function(r){if(r.avg_daily_revenue>maxH)maxH=r.avg_daily_revenue});
    var hh='<table><thead><tr><th>Hour</th><th class="text-right">Revenue</th><th class="text-right">Items</th><th class="text-right">Avg Daily Rev</th><th style="width:35%">Distribution</th></tr></thead><tbody>';
    hourly.forEach(function(r){
      var w=maxH>0?(r.avg_daily_revenue/maxH*100):0;
      hh+='<tr><td><strong>'+fmtHour(r.hour)+'</strong></td>';
      hh+='<td class="amount">'+fmt(r.revenue)+'</td>';
      hh+='<td class="amount">'+(r.items||0).toLocaleString()+'</td>';
      hh+='<td class="amount">'+fmt(r.avg_daily_revenue)+'</td>';
      hh+='<td><div class="bar-cell"><div class="bar-track"><div class="bar-fill green" style="width:'+w.toFixed(1)+'%"></div></div></div></td></tr>';
    });
    hh+='</tbody></table>';
    $('hourlyBody').innerHTML=hh;
  }

  // Expose to global scope for onclick
  window.loadMenuMix=loadMenuMix;

  // Auto-load on page open
  loadMenuMix();
})();
</script>
</body>
</html>'''



def _events_calendar_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LOV3 Events &amp; Promotional Calendar</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e5e5;min-height:100vh}
.header{background:linear-gradient(135deg,#b91c1c,#dc2626,#f97316);padding:1.5rem 2rem;text-align:center}
.header h1{font-size:1.6rem;font-weight:700;color:#fff;letter-spacing:0.5px}
.header p{color:rgba(255,255,255,.8);font-size:.85rem;margin-top:.25rem}
.nav-bar{display:flex;gap:.5rem;padding:.75rem 2rem;background:#1a1a1a;border-bottom:1px solid #333;flex-wrap:wrap}
.nav-bar a{color:#999;text-decoration:none;padding:.4rem .9rem;border-radius:6px;font-size:.82rem;transition:all .15s}
.nav-bar a:hover{color:#fff;background:#333}
.nav-bar a.active{color:#fff;background:#dc2626;font-weight:600}
.container{max-width:1400px;margin:0 auto;padding:1.5rem}
.year-toggle{display:flex;gap:.5rem;justify-content:center;margin-bottom:1.5rem}
.year-btn{padding:.5rem 1.5rem;border:2px solid #444;background:transparent;color:#ccc;border-radius:8px;cursor:pointer;font-size:.9rem;font-weight:600;transition:all .15s}
.year-btn:hover{border-color:#dc2626;color:#fff}
.year-btn.active{background:#dc2626;border-color:#dc2626;color:#fff}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1rem;margin-bottom:2rem}
.kpi-card{background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1.2rem;text-align:center}
.kpi-card .label{font-size:.72rem;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:.4rem}
.kpi-card .value{font-size:1.5rem;font-weight:700;color:#f97316}
.kpi-card .value.red{color:#dc2626}
.kpi-card .value.green{color:#22c55e}
.kpi-card .sub{font-size:.72rem;color:#666;margin-top:.25rem}
.section{margin-bottom:2rem}
.section-title{font-size:1.1rem;font-weight:700;margin-bottom:1rem;color:#fff;border-bottom:2px solid #dc2626;padding-bottom:.5rem;display:inline-block}
/* Calendar grid */
.cal-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:1.5rem;margin-bottom:2rem}
@media(max-width:900px){.cal-grid{grid-template-columns:1fr}}
.cal-month{background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1rem;overflow:hidden}
.cal-month-header{text-align:center;font-weight:700;font-size:.95rem;margin-bottom:.75rem;color:#f97316}
.cal-days{display:grid;grid-template-columns:repeat(7,1fr);gap:2px}
.cal-dow{text-align:center;font-size:.65rem;color:#666;padding:.25rem 0;font-weight:600}
.cal-day{text-align:center;font-size:.75rem;padding:.35rem .15rem;border-radius:4px;position:relative;min-height:2rem;display:flex;flex-direction:column;align-items:center;justify-content:center;cursor:default}
.cal-day.empty{opacity:0}
.cal-day.today{outline:2px solid #dc2626;outline-offset:-2px;font-weight:700;color:#fff}
.cal-day.peak-week{background:rgba(234,179,8,.08)}
.cal-day .dots{display:flex;gap:2px;margin-top:2px;justify-content:center;flex-wrap:wrap}
.cal-day .dot{width:5px;height:5px;border-radius:50%}
.dot-holiday{background:#ef4444}
.dot-conference{background:#3b82f6}
.dot-cultural{background:#f59e0b}
.dot-lov3{background:#a855f7}
.dot-sports{background:#22c55e}
.cal-day .tooltip{display:none;position:absolute;bottom:100%;left:50%;transform:translateX(-50%);background:#333;color:#fff;padding:.4rem .6rem;border-radius:6px;font-size:.68rem;white-space:nowrap;z-index:10;pointer-events:none}
.cal-day:hover .tooltip{display:block}
/* Tables */
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{text-align:left;padding:.6rem .8rem;background:#1a1a1a;color:#888;font-weight:600;text-transform:uppercase;font-size:.7rem;letter-spacing:.5px;border-bottom:1px solid #333}
td{padding:.6rem .8rem;border-bottom:1px solid #222;color:#ccc}
tr:hover td{background:#1a1a1a}
.badge{display:inline-block;padding:.15rem .5rem;border-radius:4px;font-size:.7rem;font-weight:600;color:#fff}
.badge-holiday{background:#991b1b}
.badge-conference{background:#1e40af}
.badge-cultural{background:#92400e}
.badge-lov3{background:#7e22ce}
.badge-sports{background:#166534}
.bar-cell{position:relative}
.bar-fill{position:absolute;left:0;top:0;bottom:0;border-radius:0 4px 4px 0;opacity:.15}
.bar-fill-purple{background:#a855f7}
/* Insights */
.insights-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:1rem}
.insight-card{background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1.2rem}
.insight-card .insight-title{font-weight:700;color:#f97316;margin-bottom:.5rem;font-size:.9rem}
.insight-card .insight-text{font-size:.82rem;color:#bbb;line-height:1.5}
.loading{text-align:center;color:#666;padding:3rem;font-size:.9rem}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Events &amp; Promotional Calendar</h1>
  <p>Forward-looking event planning &amp; historical revenue overlay</p>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events" class="active">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="year-toggle">
    <button class="year-btn" data-year="2025" onclick="window.loadEvents(2025)">2025</button>
    <button class="year-btn" data-year="2026" onclick="window.loadEvents(2026)">2026</button>
  </div>

  <div id="kpiRow" class="kpi-row"><div class="loading">Loading...</div></div>

  <div class="section">
    <div class="section-title">6-Month Calendar</div>
    <div id="calGrid" class="cal-grid"><div class="loading">Loading calendar...</div></div>
  </div>

  <div class="section">
    <div class="section-title">Upcoming Events</div>
    <div id="upcomingSection"><div class="loading">Loading...</div></div>
  </div>

  <div class="section">
    <div class="section-title">Top 20 Revenue Weeks</div>
    <div id="topWeeksSection"><div class="loading">Loading...</div></div>
  </div>

  <div class="section">
    <div class="section-title">Insights &amp; Intel</div>
    <div id="insightsSection" class="insights-grid"><div class="loading">Loading...</div></div>
  </div>
</div>

<script>
(function(){
  const $=id=>document.getElementById(id);
  const fmt$=v=>'$'+(v>=1000?(v/1000).toFixed(1)+'K':Math.round(v));
  const fmtN=v=>v!=null?v.toLocaleString():'--';
  const MONTHS=['January','February','March','April','May','June','July','August','September','October','November','December'];
  const DOW=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const CAT_COLORS={holiday:'#ef4444',conference:'#3b82f6',cultural:'#f59e0b',lov3:'#a855f7',sports:'#22c55e'};

  let cachedData={};
  let currentYear=new Date().getFullYear();
  let calOffset=0; // offset in 6-month pages

  function loadEvents(year){
    currentYear=year;
    calOffset=0;
    document.querySelectorAll('.year-btn').forEach(b=>{
      b.classList.toggle('active',parseInt(b.dataset.year)===year);
    });
    if(cachedData[year]){
      render(cachedData[year]);
      return;
    }
    $('kpiRow').innerHTML='<div class="loading">Loading...</div>';
    $('calGrid').innerHTML='<div class="loading">Loading calendar...</div>';
    $('upcomingSection').innerHTML='<div class="loading">Loading...</div>';
    $('topWeeksSection').innerHTML='<div class="loading">Loading...</div>';
    $('insightsSection').innerHTML='<div class="loading">Loading...</div>';

    fetch('/api/events-calendar',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({year:year})})
      .then(r=>r.json())
      .then(d=>{
        if(d.error){$('kpiRow').innerHTML='<div class="loading" style="color:#ef4444">Error: '+d.error+'</div>';return;}
        cachedData[year]=d;
        render(d);
      })
      .catch(e=>{$('kpiRow').innerHTML='<div class="loading" style="color:#ef4444">'+e+'</div>';});
  }

  function render(d){
    renderKPIs(d.kpis);
    renderCalendar(d);
    renderUpcoming(d.upcoming_events);
    renderTopWeeks(d.top_weeks);
    renderInsights(d.insights);
  }

  function renderKPIs(k){
    if(!k){$('kpiRow').innerHTML='<div class="loading">No data</div>';return;}
    $('kpiRow').innerHTML=`
      <div class="kpi-card"><div class="label">Next Major Event</div><div class="value red">${k.next_event||'--'}</div><div class="sub">${k.next_event_date||''}</div></div>
      <div class="kpi-card"><div class="label">Days Until</div><div class="value">${k.days_until!=null?k.days_until:'--'}</div></div>
      <div class="kpi-card"><div class="label">Best Month (All-Time)</div><div class="value green">${k.best_month||'--'}</div><div class="sub">${k.best_month_revenue?fmt$(k.best_month_revenue):''}</div></div>
      <div class="kpi-card"><div class="label">Avg Weekly Revenue</div><div class="value">${k.avg_weekly_revenue?fmt$(k.avg_weekly_revenue):'--'}</div></div>
      <div class="kpi-card"><div class="label">Peak Week Revenue</div><div class="value green">${k.peak_week_revenue?fmt$(k.peak_week_revenue):'--'}</div><div class="sub">${k.peak_week_date||''}</div></div>
    `;
  }

  function renderCalendar(d){
    // Show 6 months starting from Jan of selected year + calOffset*6
    const startMonth=calOffset*6;
    const events=d.events||[];
    const weeklyRev=d.weekly_revenue||[];
    const priorWeekly=d.prior_year_weekly||[];
    const allWeekly=[...weeklyRev,...priorWeekly];

    // Build set of peak-week dates (top 20 weeks)
    const topWeekStarts=new Set((d.top_weeks||[]).map(w=>w.week_start));
    const peakDays=new Set();
    topWeekStarts.forEach(ws=>{
      const sd=new Date(ws+'T00:00:00');
      for(let i=0;i<7;i++){
        const dd=new Date(sd);dd.setDate(dd.getDate()+i);
        peakDays.add(dd.toISOString().slice(0,10));
      }
    });

    // Build event lookup by date
    const eventsByDate={};
    events.forEach(ev=>{
      const s=new Date(ev.start_date+'T00:00:00');
      const e=new Date(ev.end_date+'T00:00:00');
      for(let d=new Date(s);d<=e;d.setDate(d.getDate()+1)){
        const key=d.toISOString().slice(0,10);
        if(!eventsByDate[key])eventsByDate[key]=[];
        eventsByDate[key].push(ev);
      }
    });

    const today=new Date().toISOString().slice(0,10);
    let html='';
    for(let mi=startMonth;mi<startMonth+6&&mi<12;mi++){
      const yr=currentYear;
      const firstDay=new Date(yr,mi,1);
      const daysInMonth=new Date(yr,mi+1,0).getDate();
      const startDow=firstDay.getDay();

      html+=`<div class="cal-month"><div class="cal-month-header">${MONTHS[mi]} ${yr}</div><div class="cal-days">`;
      DOW.forEach(d=>{html+=`<div class="cal-dow">${d}</div>`;});

      for(let i=0;i<startDow;i++) html+=`<div class="cal-day empty"></div>`;

      for(let day=1;day<=daysInMonth;day++){
        const dateStr=yr+'-'+(mi+1<10?'0':'')+(mi+1)+'-'+(day<10?'0':'')+day;
        const isToday=dateStr===today;
        const isPeak=peakDays.has(dateStr);
        const dayEvents=eventsByDate[dateStr]||[];
        let cls='cal-day';
        if(isToday) cls+=' today';
        if(isPeak) cls+=' peak-week';

        let dots='';
        let tooltipText='';
        if(dayEvents.length>0){
          const seen=new Set();
          dayEvents.forEach(ev=>{
            if(!seen.has(ev.category)){
              dots+=`<span class="dot dot-${ev.category}"></span>`;
              seen.add(ev.category);
            }
          });
          tooltipText=dayEvents.map(ev=>ev.name).join(', ');
        }

        html+=`<div class="${cls}">${day}`;
        if(dots) html+=`<div class="dots">${dots}</div>`;
        if(tooltipText) html+=`<div class="tooltip">${tooltipText}</div>`;
        html+=`</div>`;
      }
      html+=`</div></div>`;
    }
    $('calGrid').innerHTML=html;
  }

  function renderUpcoming(events){
    if(!events||events.length===0){$('upcomingSection').innerHTML='<p style="color:#666">No upcoming events</p>';return;}
    let html='<table><thead><tr><th>Date</th><th>Event</th><th>Category</th><th>Duration</th><th>Historical Revenue Context</th></tr></thead><tbody>';
    events.forEach(ev=>{
      const s=new Date(ev.start_date+'T00:00:00');
      const e=new Date(ev.end_date+'T00:00:00');
      const days=Math.round((e-s)/(86400000))+1;
      const dur=days===1?'1 day':days+' days';
      const dateStr=s.toLocaleDateString('en-US',{month:'short',day:'numeric'})+(days>1?' - '+e.toLocaleDateString('en-US',{month:'short',day:'numeric'}):'');
      const ctx=ev.historical_revenue?fmt$(ev.historical_revenue)+' peak week':'--';
      html+=`<tr><td>${dateStr}</td><td>${ev.name}</td><td><span class="badge badge-${ev.category}">${ev.category}</span></td><td>${dur}</td><td>${ctx}</td></tr>`;
    });
    html+='</tbody></table>';
    $('upcomingSection').innerHTML=html;
  }

  function renderTopWeeks(weeks){
    if(!weeks||weeks.length===0){$('topWeeksSection').innerHTML='<p style="color:#666">No revenue data</p>';return;}
    const maxRev=Math.max(...weeks.map(w=>w.revenue||0));
    let html='<table><thead><tr><th>#</th><th>Week Starting</th><th>Revenue</th><th>Orders</th><th style="min-width:200px">Overlapping Events</th></tr></thead><tbody>';
    weeks.forEach(w=>{
      const pct=maxRev>0?((w.revenue||0)/maxRev*100):0;
      const evTags=(w.events||[]).map(e=>`<span class="badge badge-${e.category}">${e.name}</span>`).join(' ');
      const ws=new Date(w.week_start+'T00:00:00');
      const dateStr=ws.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'});
      html+=`<tr><td>${w.rank}</td><td>${dateStr}</td><td class="bar-cell"><div class="bar-fill bar-fill-purple" style="width:${pct}%"></div>${fmt$(w.revenue||0)}</td><td>${fmtN(w.orders)}</td><td>${evTags||'--'}</td></tr>`;
    });
    html+='</tbody></table>';
    $('topWeeksSection').innerHTML=html;
  }

  function renderInsights(insights){
    if(!insights||insights.length===0){$('insightsSection').innerHTML='<p style="color:#666">No insights available</p>';return;}
    let html='';
    insights.forEach(ins=>{
      html+=`<div class="insight-card"><div class="insight-title">${ins.title}</div><div class="insight-text">${ins.text}</div></div>`;
    });
    $('insightsSection').innerHTML=html;
  }

  window.loadEvents=loadEvents;

  // Auto-load current year
  loadEvents(currentYear);
})();
</script>
</body>
</html>'''



def _customer_loyalty_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LOV3 Guest Intelligence</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e5e5;min-height:100vh}
.header{background:linear-gradient(135deg,#0d9488,#14b8a6,#2dd4bf);padding:1.5rem 2rem;text-align:center}
.header h1{font-size:1.6rem;font-weight:700;color:#fff;letter-spacing:0.5px}
.header p{color:rgba(255,255,255,.8);font-size:.85rem;margin-top:.25rem}
.nav-bar{display:flex;gap:.5rem;padding:.75rem 2rem;background:#1a1a1a;border-bottom:1px solid #333;flex-wrap:wrap}
.nav-bar a{color:#999;text-decoration:none;padding:.4rem .9rem;border-radius:6px;font-size:.82rem;transition:all .15s}
.nav-bar a:hover{color:#fff;background:#333}
.nav-bar a.active{color:#fff;background:#14b8a6;font-weight:600}
.container{max-width:1400px;margin:0 auto;padding:1.5rem}
.filter-bar{display:flex;gap:1rem;align-items:center;margin-bottom:1.5rem;flex-wrap:wrap}
.filter-bar label{font-size:.82rem;color:#999}
.filter-bar input[type=date]{background:#1e1e1e;border:1px solid #444;color:#fff;padding:.4rem .6rem;border-radius:6px;font-size:.82rem}
.filter-bar button{background:#14b8a6;color:#fff;border:none;padding:.5rem 1.5rem;border-radius:6px;font-weight:600;cursor:pointer;font-size:.85rem;transition:background .15s}
.filter-bar button:hover{background:#0d9488}
.banner{background:#1e1e1e;border:1px solid #333;border-left:4px solid #14b8a6;border-radius:8px;padding:1rem 1.5rem;margin-bottom:1.5rem;font-size:.85rem;color:#bbb}
.banner strong{color:#14b8a6}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin-bottom:2rem}
.kpi-card{background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1.1rem;text-align:center}
.kpi-card .label{font-size:.7rem;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:.3rem}
.kpi-card .value{font-size:1.4rem;font-weight:700;color:#2dd4bf}
.kpi-card .value.warn{color:#f59e0b}
.kpi-card .value.red{color:#ef4444}
.kpi-card .sub{font-size:.7rem;color:#666;margin-top:.2rem}
.seg-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:2rem}
.seg-card{background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1.2rem;text-align:center;border-top:4px solid #374151}
.seg-card .seg-title{font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;margin-bottom:.5rem}
.seg-card .seg-count{font-size:1.6rem;font-weight:700;color:#fff}
.seg-card .seg-detail{font-size:.75rem;color:#888;margin-top:.3rem}
.section{margin-bottom:2rem}
.section-title{font-size:1.05rem;font-weight:700;margin-bottom:1rem;color:#fff;border-bottom:2px solid #14b8a6;padding-bottom:.5rem;display:inline-block}
table{width:100%;border-collapse:collapse;font-size:.8rem}
th{text-align:left;padding:.55rem .7rem;background:#1a1a1a;color:#888;font-weight:600;text-transform:uppercase;font-size:.68rem;letter-spacing:.5px;border-bottom:1px solid #333}
td{padding:.55rem .7rem;border-bottom:1px solid #222;color:#ccc}
tr:hover td{background:#1a1a1a}
.badge{display:inline-block;padding:.15rem .5rem;border-radius:4px;font-size:.68rem;font-weight:600;color:#fff}
.badge-champions{background:#92400e}
.badge-loyal{background:#78350f}
.badge-regulars{background:#115e59}
.badge-returning{background:#1e40af}
.badge-new{background:#166534}
.badge-at_risk{background:#9a3412}
.badge-dormant{background:#374151}
.bar-fill{height:10px;border-radius:5px;transition:width .3s}
.conc-row{display:flex;align-items:center;gap:.75rem;margin-bottom:.6rem}
.conc-label{width:70px;font-size:.8rem;color:#999;text-align:right;flex-shrink:0}
.conc-bar{flex:1;background:#222;border-radius:5px;height:10px;overflow:hidden}
.conc-fill{height:100%;border-radius:5px;background:linear-gradient(90deg,#14b8a6,#2dd4bf)}
.conc-val{width:120px;font-size:.8rem;color:#ccc;flex-shrink:0}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}
@media(max-width:900px){.two-col{grid-template-columns:1fr}}
.loading{text-align:center;color:#666;padding:3rem;font-size:.9rem}
.empty{text-align:center;color:#555;padding:2rem;font-size:.85rem;font-style:italic}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Guest Intelligence</h1>
  <p>Card-based guest segmentation, visit behavior &amp; revenue analytics</p>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty" class="active">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label>
    <input type="date" id="startDate">
    <label>To</label>
    <input type="date" id="endDate">
    <button onclick="window.loadLoyalty()">Analyze</button>
  </div>

  <div id="bannerEl" class="banner" style="display:none"></div>
  <div id="kpiRow" class="kpi-row"><div class="loading">Loading...</div></div>
  <div id="segGrid" class="seg-grid" style="display:none"></div>
  <div id="concSection" class="section" style="display:none"></div>
  <div id="freqSection" class="section" style="display:none"></div>
  <div id="monthlySection" class="section" style="display:none"></div>
  <div id="patternsSection" class="section" style="display:none"></div>
  <div id="topSection" class="section" style="display:none"></div>
  <div id="contactsSection" class="section" style="display:none"></div>
  <div id="marketingSection" class="section" style="display:none"></div>
  <div id="sevenroomsSection" class="section" style="display:none"></div>
</div>

<script>
(function(){
  const $ = id => document.getElementById(id);
  const fmt = n => Number(n||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  const fmtD = n => Number(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  const fmtPct = n => Number(n||0).toFixed(1)+'%';
  const esc = s => {if(!s)return '';return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');};

  const SEG_META = {
    champions:{label:'Champions',color:'#f59e0b',desc:'10+ visits, seen in last 30 days'},
    loyal:{label:'Loyal',color:'#d97706',desc:'10+ visits, not seen recently'},
    regulars:{label:'Regulars',color:'#14b8a6',desc:'5-9 visits, active'},
    returning:{label:'Returning',color:'#3b82f6',desc:'2-4 visits'},
    new:{label:'New Guests',color:'#22c55e',desc:'First visit in last 30 days'},
    at_risk:{label:'At Risk',color:'#f97316',desc:'3+ visits but 45-90 days absent'},
    dormant:{label:'Dormant',color:'#6b7280',desc:'90+ days since last visit'}
  };

  const now = new Date();
  const six = new Date(now); six.setMonth(six.getMonth()-6);
  $('startDate').value = six.toISOString().slice(0,10);
  $('endDate').value = now.toISOString().slice(0,10);

  window.loadLoyalty = async function(){
    const s=$('startDate').value, e=$('endDate').value;
    if(!s||!e) return;
    $('kpiRow').innerHTML='<div class="loading">Analyzing guest data across '+fmt(0)+' cards...</div>';
    ['bannerEl','segGrid','concSection','freqSection','monthlySection','patternsSection','topSection'].forEach(id=>{$(id).style.display='none';});
    try {
      const resp = await fetch('/api/customer-loyalty',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:s,end_date:e})});
      if(!resp.ok) throw new Error((await resp.json()).error||resp.statusText);
      renderAll(await resp.json());
    } catch(err){
      $('kpiRow').innerHTML='<div class="loading" style="color:#ef4444">Error: '+err.message+'</div>';
    }
  };

  function renderAll(d){
    const k=d.kpis;

    /* Banner */
    const bn=$('bannerEl');
    bn.innerHTML='Tracking <strong>'+fmt(k.total_guests)+'</strong> unique guest cards across <strong>'+fmt(k.total_txns)+'</strong> transactions ($'+fmtD(k.total_revenue)+' revenue). Guest identification uses card last-4 digits + card type as proxy.';
    bn.style.display='block';

    /* KPIs */
    $('kpiRow').innerHTML=[
      kpi('Unique Guests',fmt(k.total_guests),'Card-based'),
      kpi('Repeat Rate',fmtPct(k.repeat_pct),'Guests with 2+ visits'),
      kpi('Repeat Revenue',fmtPct(k.repeat_rev_pct),'From returning guests','warn'),
      kpi('Avg Visits',k.avg_visits_repeat,'Per returning guest'),
      kpi('Avg Spend/Visit','$'+fmtD(k.avg_spend_per_visit),''),
      kpi('Revenue/Guest','$'+fmtD(k.rev_per_guest),'Lifetime proxy'),
      kpi('At Risk',fmt(k.at_risk_count),'$'+fmtD(k.at_risk_revenue)+' at stake','red'),
    ].join('');

    /* Segments */
    renderSegments(d.segments);

    /* Concentration */
    renderConcentration(d.concentration, k.total_guests);

    /* Frequency distribution */
    renderFreqDist(d.freq_distribution);

    /* Monthly trend */
    renderMonthly(d.monthly);

    /* DOW + Hourly */
    renderPatterns(d.patterns);

    /* Top guests */
    renderTopGuests(d.top_guests);

    /* Contacts & Marketing */
    renderContacts(d.contacts);
    renderMarketing(d.segments, d.contacts);
    renderSevenRooms();
  }

  function kpi(label,value,sub,cls){
    return '<div class="kpi-card"><div class="label">'+label+'</div><div class="value'+(cls?' '+cls:'')+'">'+value+'</div>'+(sub?'<div class="sub">'+sub+'</div>':'')+'</div>';
  }

  function renderSegments(segs){
    const el=$('segGrid');
    let h='';
    for(const[key,meta] of Object.entries(SEG_META)){
      const s=segs[key]; if(!s||!s.count) continue;
      h+='<div class="seg-card" style="border-top-color:'+meta.color+'"><div class="seg-title" style="color:'+meta.color+'">'+meta.label+'</div><div class="seg-count">'+fmt(s.count)+'</div><div class="seg-detail">'+fmtPct(s.pct_of_guests)+' of guests</div><div class="seg-detail">$'+fmtD(s.revenue)+' ('+fmtPct(s.revenue_pct)+' of rev)</div><div class="seg-detail">Avg $'+fmtD(s.avg_spend)+'/guest</div><div class="seg-detail" style="color:#555;font-size:.68rem;margin-top:.3rem">'+meta.desc+'</div></div>';
    }
    el.innerHTML=h;
    el.style.display='grid';
  }

  function renderConcentration(conc, total){
    const el=$('concSection');
    let h='<div class="section-title">Revenue Concentration</div>';
    h+='<div style="max-width:700px">';
    conc.forEach(c=>{
      h+='<div class="conc-row"><div class="conc-label">'+c.label+'</div><div class="conc-bar"><div class="conc-fill" style="width:'+c.revenue_pct+'%"></div></div><div class="conc-val"><strong>'+fmtPct(c.revenue_pct)+'</strong> of revenue ('+fmt(c.guests)+' guests)</div></div>';
    });
    h+='</div>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderFreqDist(dist){
    const el=$('freqSection');
    const maxPct=Math.max(...dist.map(d=>d.pct_revenue));
    let h='<div class="section-title">Visit Frequency Distribution</div>';
    h+='<table><thead><tr><th>Frequency</th><th>Guests</th><th>% of Guests</th><th>Revenue</th><th>% of Revenue</th><th>Avg Spend/Guest</th><th style="width:150px">Revenue Share</th></tr></thead><tbody>';
    dist.forEach(d=>{
      const w=maxPct?Math.round(d.pct_revenue/maxPct*100):0;
      h+='<tr><td style="font-weight:600">'+d.band+'</td><td>'+fmt(d.guests)+'</td><td>'+fmtPct(d.pct_guests)+'</td><td>$'+fmtD(d.revenue)+'</td><td>'+fmtPct(d.pct_revenue)+'</td><td>$'+fmtD(d.avg_spend)+'</td><td><div class="bar-fill" style="width:'+w+'%;background:#14b8a6"></div></td></tr>';
    });
    h+='</tbody></table>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderMonthly(monthly){
    const el=$('monthlySection');
    let h='<div class="section-title">Monthly Guest Trend</div>';
    h+='<table><thead><tr><th>Month</th><th>Active Guests</th><th>New</th><th>Returning</th><th>Return %</th><th>Revenue</th><th>Repeat Rev</th><th>Repeat Rev %</th></tr></thead><tbody>';
    monthly.forEach(m=>{
      h+='<tr><td>'+m.month+'</td><td>'+fmt(m.active)+'</td><td style="color:#22c55e">'+fmt(m.new)+'</td><td style="color:#f59e0b">'+fmt(m.returning)+'</td><td>'+fmtPct(m.return_pct)+'</td><td>$'+fmtD(m.revenue)+'</td><td>$'+fmtD(m.repeat_revenue)+'</td><td style="font-weight:600;color:#2dd4bf">'+fmtPct(m.repeat_rev_pct)+'</td></tr>';
    });
    h+='</tbody></table>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderPatterns(p){
    const el=$('patternsSection');
    let h='<div class="section-title">Guest Timing Patterns</div>';
    h+='<div class="two-col">';

    /* DOW */
    if(p.day_of_week&&p.day_of_week.length){
      const maxT=Math.max(...p.day_of_week.map(d=>d.txns));
      h+='<div><h4 style="color:#ccc;font-size:.88rem;margin-bottom:.6rem">Day of Week</h4><table><thead><tr><th>Day</th><th>Txns</th><th>Revenue</th><th>Champions</th><th>Regulars</th><th style="width:100px"></th></tr></thead><tbody>';
      p.day_of_week.forEach(d=>{
        const w=maxT?Math.round(d.txns/maxT*100):0;
        h+='<tr><td>'+d.day+'</td><td>'+fmt(d.txns)+'</td><td>$'+fmtD(d.revenue)+'</td><td style="color:#f59e0b">'+fmt(d.champions_txns||0)+'</td><td style="color:#14b8a6">'+fmt(d.regulars_txns||0)+'</td><td><div class="bar-fill" style="width:'+w+'%;background:#14b8a6"></div></td></tr>';
      });
      h+='</tbody></table></div>';
    }

    /* Hourly */
    if(p.hourly&&p.hourly.length){
      const maxH=Math.max(...p.hourly.map(h=>h.txns));
      h+='<div><h4 style="color:#ccc;font-size:.88rem;margin-bottom:.6rem">Hourly Profile</h4><table><thead><tr><th>Hour</th><th>Txns</th><th>Revenue</th><th style="width:120px"></th></tr></thead><tbody>';
      p.hourly.forEach(hr=>{
        if(hr.txns<1)return;
        const w=maxH?Math.round(hr.txns/maxH*100):0;
        h+='<tr><td>'+hr.label+'</td><td>'+fmt(hr.txns)+'</td><td>$'+fmtD(hr.revenue)+'</td><td><div class="bar-fill" style="width:'+w+'%;background:#2dd4bf"></div></td></tr>';
      });
      h+='</tbody></table></div>';
    }
    h+='</div>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderTopGuests(guests){
    const el=$('topSection');
    if(!guests||!guests.length){el.innerHTML='<div class="empty">No repeat guest data.</div>';el.style.display='block';return;}
    let h='<div class="section-title">Top 50 Repeat Guests</div>';
    h+='<div style="overflow-x:auto"><table><thead><tr><th>#</th><th>Card</th><th>Type</th><th>Visit Days</th><th>Transactions</th><th>Total Spend</th><th>Avg/Visit</th><th>Tip %</th><th>First Seen</th><th>Last Seen</th><th>Segment</th></tr></thead><tbody>';
    guests.forEach((g,i)=>{
      const meta=SEG_META[g.segment]||{label:g.segment,color:'#666'};
      h+='<tr><td>'+(i+1)+'</td><td style="font-family:monospace">****'+esc(g.card)+'</td><td>'+esc(g.card_type)+'</td><td style="font-weight:700">'+g.visit_days+'</td><td>'+g.txn_count+'</td><td>$'+fmtD(g.total_spend)+'</td><td>$'+fmtD(g.avg_per_visit)+'</td><td>'+fmtPct(g.tip_pct)+'</td><td>'+g.first_seen+'</td><td>'+g.last_seen+'</td><td><span class="badge badge-'+g.segment+'" style="background:'+meta.color+'">'+meta.label+'</span></td></tr>';
    });
    h+='</tbody></table></div>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function exportCSV(){
    const s=$('startDate').value, e=$('endDate').value;
    if(s&&e) window.open('/api/guest-export?start_date='+s+'&end_date='+e);
  }

  function renderContacts(ct){
    const el=$('contactsSection');
    if(!ct||!ct.total){el.innerHTML='<div class="empty">No customer contact data in this period.</div>';el.style.display='block';return;}
    let h='<div class="section-title">Contact Database</div>';
    /* mini KPIs + export */
    h+='<div style="display:flex;gap:1rem;align-items:center;margin-bottom:1rem;flex-wrap:wrap">';
    h+='<div class="kpi-card" style="flex:1;min-width:140px"><div class="label">Total Contacts</div><div class="value">'+fmt(ct.total)+'</div></div>';
    h+='<div class="kpi-card" style="flex:1;min-width:140px"><div class="label">With Email</div><div class="value">'+fmt(ct.with_email)+'</div></div>';
    h+='<div class="kpi-card" style="flex:1;min-width:140px"><div class="label">With Phone</div><div class="value">'+fmt(ct.with_phone)+'</div></div>';
    h+='<div style="flex:1;min-width:200px;text-align:center"><button onclick="exportCSV()" style="background:#14b8a6;color:#fff;border:none;padding:.7rem 2rem;border-radius:8px;font-weight:700;cursor:pointer;font-size:.9rem">Export CSV for SevenRooms</button><div style="font-size:.7rem;color:#666;margin-top:.3rem">Includes name, email, phone, segment, tags</div></div>';
    h+='</div>';
    /* Contact table - top 50 */
    const guests=ct.guests||[];
    if(guests.length){
      h+='<div style="overflow-x:auto"><table><thead><tr><th>#</th><th>Name</th><th>Email</th><th>Phone</th><th>Visits</th><th>Total Spend</th><th>Avg Check</th><th>Last Visit</th><th>Segment</th><th>Card Link</th></tr></thead><tbody>';
      guests.slice(0,50).forEach((g,i)=>{
        const meta=SEG_META[g.segment]||{label:g.segment,color:'#666'};
        h+='<tr><td>'+(i+1)+'</td><td>'+esc(g.name)+'</td><td style="font-size:.75rem">'+esc(g.email)+'</td><td style="font-family:monospace;font-size:.75rem">'+esc(g.phone)+'</td><td>'+g.visits+'</td><td>$'+fmtD(g.total_spend)+'</td><td>$'+fmtD(g.avg_check)+'</td><td>'+g.last_visit+'</td><td><span class="badge" style="background:'+(meta.color||'#666')+'">'+meta.label+'</span></td><td style="font-family:monospace;font-size:.75rem">'+(g.linked_card?'****'+esc(g.linked_card):'-')+'</td></tr>';
      });
      h+='</tbody></table></div>';
      if(guests.length>50) h+='<div style="text-align:center;color:#666;font-size:.8rem;margin-top:.5rem">Showing top 50 of '+fmt(guests.length)+' contacts. Download CSV for full list.</div>';
    }
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderMarketing(segs, ct){
    const el=$('marketingSection');
    const guests=(ct&&ct.guests)||[];
    /* Count contacts with email per segment */
    const segEmail={};
    const segRev={};
    guests.forEach(g=>{
      if(!segEmail[g.segment]) segEmail[g.segment]=0;
      if(!segRev[g.segment]) segRev[g.segment]=0;
      if(g.email) segEmail[g.segment]++;
      segRev[g.segment]+=g.total_spend||0;
    });

    const campaigns=[
      {seg:'champions',title:'VIP Rewards & Referrals',color:'#f59e0b',channel:'Email + SMS',priority:'High',
       desc:'Exclusive event invites, VIP perks, refer-a-friend program. These are your brand ambassadors — reward their loyalty and turn them into advocates.'},
      {seg:'loyal',title:'Appreciation Campaigns',color:'#d97706',channel:'Email',priority:'High',
       desc:'Birthday/anniversary offers, early access to new menu items, personal thank-you from management. They love you — show them you notice.'},
      {seg:'regulars',title:'Frequency Builders',color:'#14b8a6',channel:'Email + SMS',priority:'Medium',
       desc:'Visit incentives ("Come in 2 more times this month for..."), upsell premium items, loyalty program enrollment. Build the habit.'},
      {seg:'returning',title:'Welcome Back Series',color:'#3b82f6',channel:'Email',priority:'Medium',
       desc:'Post-visit thank you, personalized menu recommendations based on past orders, second-visit discount. Nurture the relationship.'},
      {seg:'at_risk',title:'Win-Back Campaign',color:'#f97316',channel:'SMS + Email',priority:'Urgent',
       desc:'"We miss you" message with limited-time incentive, personal invitation to upcoming event. Act now before they become dormant.'},
      {seg:'dormant',title:'Reactivation Offer',color:'#6b7280',channel:'Email',priority:'Low',
       desc:'Major incentive ("50% off your next visit"), "Here\\\'s what\\\'s new since you\\\'ve been gone" showcase. Last chance before they\\\'re lost.'},
    ];

    let h='<div class="section-title">Marketing Campaign Playbook</div>';
    h+='<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:1rem">';
    campaigns.forEach(c=>{
      const emails=segEmail[c.seg]||0;
      const rev=segRev[c.seg]||0;
      const segInfo=segs[c.seg]||{};
      if(!segInfo.count && !emails) return;
      h+='<div style="background:#1e1e1e;border:1px solid #333;border-top:4px solid '+c.color+';border-radius:10px;padding:1.2rem">';
      h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.5rem"><div style="font-weight:700;color:'+c.color+';font-size:.9rem">'+c.title+'</div><span class="badge" style="background:'+(c.priority==='Urgent'?'#ef4444':c.priority==='High'?'#f59e0b':'#374151')+'">'+c.priority+'</span></div>';
      h+='<div style="font-size:.8rem;color:#bbb;line-height:1.5;margin-bottom:.75rem">'+c.desc+'</div>';
      h+='<div style="display:flex;gap:1rem;font-size:.75rem;color:#888;border-top:1px solid #333;padding-top:.6rem">';
      h+='<div>Reachable: <strong style="color:#fff">'+fmt(emails)+'</strong> emails</div>';
      h+='<div>Revenue: <strong style="color:#fff">$'+fmtD(rev)+'</strong></div>';
      h+='<div>Channel: <strong style="color:'+c.color+'">'+c.channel+'</strong></div>';
      h+='</div></div>';
    });
    h+='</div>';
    el.innerHTML=h;
    el.style.display='block';
  }

  function renderSevenRooms(){
    const el=$('sevenroomsSection');
    let h='<div class="section-title">SevenRooms CRM Integration</div>';
    h+='<div style="background:#1e1e1e;border:1px solid #333;border-radius:10px;padding:1.5rem;max-width:800px">';
    h+='<div style="font-size:.88rem;color:#ccc;line-height:1.7">';
    h+='<div style="font-weight:700;color:#14b8a6;margin-bottom:.75rem;font-size:.95rem">How to activate this data in SevenRooms</div>';
    h+='<div style="margin-bottom:.6rem"><span style="background:#14b8a6;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;margin-right:.5rem">1</span><strong>Enable Toast Integration</strong> — In SevenRooms, connect your Toast POS to auto-sync order spend and check data to guest profiles.</div>';
    h+='<div style="margin-bottom:.6rem"><span style="background:#14b8a6;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;margin-right:.5rem">2</span><strong>Export Guest CSV</strong> — Click "Export CSV for SevenRooms" above. This includes name, email, phone, visit history, spend, and segment tags.</div>';
    h+='<div style="margin-bottom:.6rem"><span style="background:#14b8a6;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;margin-right:.5rem">3</span><strong>Import to SevenRooms</strong> — Upload the CSV to your SevenRooms Guest Database. Guest profiles will be created/updated with contact info.</div>';
    h+='<div style="margin-bottom:.6rem"><span style="background:#14b8a6;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;margin-right:.5rem">4</span><strong>Create Auto-Tags</strong> — Set up matching tags in SevenRooms: Champions, Loyal, Regulars, At Risk, etc. Use the "tags" column from the CSV.</div>';
    h+='<div style="margin-bottom:.6rem"><span style="background:#14b8a6;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;margin-right:.5rem">5</span><strong>Build Campaigns</strong> — Use SevenRooms Email/SMS marketing to target segments: VIP rewards for Champions, win-back offers for At Risk, welcome series for New.</div>';
    h+='</div>';
    h+='<div style="margin-top:1rem;padding-top:.75rem;border-top:1px solid #333;font-size:.75rem;color:#666">Re-export monthly to keep segments current as guest behavior changes. SevenRooms will merge updated profiles by email match.</div>';
    h+='</div>';
    el.innerHTML=h;
    el.style.display='block';
  }

  window.loadLoyalty();
})();
</script>
</body>
</html>'''



def _server_performance_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Server Performance</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e7eb;min-height:100vh}
.header{background:linear-gradient(135deg,#059669,#10b981,#34d399);padding:24px 32px;color:#fff}
.header h1{font-size:1.5rem;font-weight:800;letter-spacing:-0.5px}.header .subtitle{font-size:0.85rem;opacity:0.9;margin-top:4px}
.nav-bar{background:#1a1a1a;border-bottom:1px solid #333;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#9ca3af;transition:all 0.15s}
.nav-bar a:hover{background:#222;color:#fff}
.nav-bar a.active{background:#10b981;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.filter-bar{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:16px 20px;margin-bottom:24px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.filter-bar label{font-size:0.82rem;color:#9ca3af;font-weight:600}
.filter-bar input[type="date"]{background:#111;border:1px solid #444;color:#e5e7eb;padding:8px 12px;border-radius:8px;font-size:0.85rem}
.filter-bar .btn{background:linear-gradient(135deg,#059669,#10b981);color:#fff;border:none;padding:8px 24px;border-radius:8px;font-weight:700;font-size:0.85rem;cursor:pointer}
.filter-bar .btn:hover{opacity:0.9}
.kpi-row{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.kpi-row{grid-template-columns:repeat(2,1fr)}}
.kpi{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#9ca3af;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}
.kpi .value{font-size:1.6rem;font-weight:800;color:#10b981;margin-top:4px;font-family:"SF Mono",monospace}
.section{background:#1e1e1e;border:1px solid #333;border-radius:12px;margin-bottom:24px;overflow:hidden}
.section-title{font-size:1.1rem;font-weight:700;padding:16px 20px;color:#fff;border-bottom:1px solid #333}
.section-body{padding:0;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem}
th{text-align:left;padding:10px 14px;background:#1a1a1a;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:0.7rem;letter-spacing:0.5px;border-bottom:1px solid #333;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #222;color:#ccc;vertical-align:middle}
tr:hover td{background:#1a1a1a}
tr.clickable{cursor:pointer}
tr.clickable:hover td{background:#0d3320}
.amount{font-family:"SF Mono",monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
.rank-num{font-weight:700;color:#10b981}
.bar-cell{display:flex;align-items:center;gap:10px}
.bar-track{flex:1;height:18px;background:#222;border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;min-width:2px;background:linear-gradient(90deg,#059669,#10b981)}
.bar-value{font-family:"SF Mono",monospace;font-size:0.8rem;font-weight:600;min-width:70px;text-align:right;color:#10b981}
.detail-panel{display:none;background:#161616;border:1px solid #333;border-radius:12px;margin-bottom:24px;overflow:hidden}
.detail-panel.active{display:block}
.detail-header{padding:16px 20px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center}
.detail-header h3{color:#10b981;font-size:1rem}
.detail-header .close-btn{background:none;border:1px solid #555;color:#999;padding:4px 12px;border-radius:6px;cursor:pointer;font-size:0.8rem}
.detail-header .close-btn:hover{color:#fff;border-color:#999}
.detail-grid{display:grid;grid-template-columns:1fr 1fr;gap:0}
@media(max-width:768px){.detail-grid{grid-template-columns:1fr}}
.detail-grid .sub-section{padding:16px 20px;border-right:1px solid #222}
.detail-grid .sub-section:last-child{border-right:none}
.detail-grid .sub-title{font-size:0.82rem;font-weight:700;color:#9ca3af;margin-bottom:12px;text-transform:uppercase;letter-spacing:0.5px}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#1e1e1e;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.3);text-align:center;color:#ccc}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #10b981;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Server Performance</h1>
  <div class="subtitle">Server rankings, tip analysis, and individual performance breakdown</div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers" class="active">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label><input type="date" id="startDate">
    <label>To</label><input type="date" id="endDate">
    <button class="btn" onclick="window.loadServerPerf()">Analyze</button>
  </div>

  <div id="kpiRow" class="kpi-row"></div>

  <div id="detailPanel" class="detail-panel">
    <div class="detail-header">
      <h3 id="detailName"></h3>
      <button class="close-btn" onclick="document.getElementById('detailPanel').classList.remove('active')">Close</button>
    </div>
    <div class="detail-grid">
      <div class="sub-section">
        <div class="sub-title">Day of Week Breakdown</div>
        <table><thead><tr><th>Day</th><th class="text-right">Revenue</th><th class="text-right">Orders</th><th class="text-right">Avg Check</th></tr></thead><tbody id="detailDow"></tbody></table>
      </div>
      <div class="sub-section">
        <div class="sub-title">Hourly Performance</div>
        <table><thead><tr><th>Hour</th><th class="text-right">Revenue</th><th class="text-right">Orders</th></tr></thead><tbody id="detailHourly"></tbody></table>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Server Leaderboard</div>
    <div class="section-body"><table><thead><tr>
      <th>#</th><th>Server</th><th>Revenue</th><th class="text-right">Orders</th><th class="text-right">Avg Check</th><th class="text-right">Guests</th><th class="text-right">Rev/Guest</th><th class="text-right">Tips</th><th class="text-right">Tip %</th><th class="text-right">Discounts</th>
    </tr></thead><tbody id="leaderboard"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Discount Analysis</div>
    <div class="section-body"><table><thead><tr>
      <th>Server</th><th class="text-right">Total Discounts</th><th class="text-right">Discount %</th><th class="text-right">Discounted Orders</th>
    </tr></thead><tbody id="discountTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Tip Analysis</div>
    <div class="section-body"><table><thead><tr>
      <th>Server</th><th class="text-right">Total Tips</th><th class="text-right">Avg Tip %</th><th class="text-right">Total Gratuity</th>
    </tr></thead><tbody id="tipTable"></tbody></table></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Analyzing server data...</div>
</div>

<script>
(function(){
  const $=id=>document.getElementById(id);
  const fmt=v=>'$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  const fmtD=v=>'$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  const pct=v=>(v||0).toFixed(1)+'%';

  // Default date range: 3 months back
  const today=new Date();
  const start=new Date(today);start.setMonth(start.getMonth()-3);
  $('startDate').value=start.toISOString().slice(0,10);
  $('endDate').value=today.toISOString().slice(0,10);

  let cachedData=null;

  async function loadServerPerf(){
    $('loadingOverlay').classList.add('active');
    try{
      const res=await fetch('/api/server-performance',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:$('startDate').value,end_date:$('endDate').value})});
      const data=await res.json();
      if(!res.ok)throw new Error(data.error||'API error');
      cachedData=data;
      renderKPIs(data.kpis);
      renderLeaderboard(data.servers);
      renderDiscounts(data.servers);
      renderTips(data.servers);
      $('detailPanel').classList.remove('active');
    }catch(e){alert('Error: '+e.message)}
    finally{$('loadingOverlay').classList.remove('active')}
  }

  function renderKPIs(k){
    const items=[
      ['Total Servers',k.total_servers],
      ['Avg Rev/Server',fmt(k.avg_revenue_per_server)],
      ['Top Server Rev',fmt(k.top_server_revenue)],
      ['Avg Check Size',fmtD(k.avg_check_size)],
      ['Avg Tip %',pct(k.avg_tip_pct)]
    ];
    $('kpiRow').innerHTML=items.map(([l,v])=>`<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');
  }

  function renderLeaderboard(servers){
    const maxRev=Math.max(...servers.map(s=>s.revenue||0),1);
    $('leaderboard').innerHTML=servers.map((s,i)=>`<tr class="clickable" onclick="window.showServerDetail('${s.server.replace(/'/g,"\\'")}')">
      <td class="rank-num">${i+1}</td>
      <td><strong>${s.server}</strong></td>
      <td><div class="bar-cell"><div class="bar-track"><div class="bar-fill" style="width:${(s.revenue/maxRev*100).toFixed(1)}%"></div></div><div class="bar-value">${fmt(s.revenue)}</div></div></td>
      <td class="amount">${(s.orders||0).toLocaleString()}</td>
      <td class="amount">${fmtD(s.avg_check)}</td>
      <td class="amount">${(s.guests||0).toLocaleString()}</td>
      <td class="amount">${fmtD(s.rev_per_guest)}</td>
      <td class="amount">${fmt(s.tips)}</td>
      <td class="amount">${pct(s.tip_pct)}</td>
      <td class="amount">${fmt(s.discounts)}</td>
    </tr>`).join('');
  }

  function renderDiscounts(servers){
    const sorted=[...servers].sort((a,b)=>(b.discounts||0)-(a.discounts||0));
    $('discountTable').innerHTML=sorted.filter(s=>s.discounts>0).map(s=>`<tr>
      <td><strong>${s.server}</strong></td>
      <td class="amount">${fmt(s.discounts)}</td>
      <td class="amount">${pct(s.discount_pct)}</td>
      <td class="amount">${(s.discounted_orders||0).toLocaleString()}</td>
    </tr>`).join('')||'<tr><td colspan="4" style="text-align:center;color:#666;padding:20px">No discounts in this period</td></tr>';
  }

  function renderTips(servers){
    const sorted=[...servers].sort((a,b)=>(b.tips||0)-(a.tips||0));
    $('tipTable').innerHTML=sorted.map(s=>`<tr>
      <td><strong>${s.server}</strong></td>
      <td class="amount">${fmt(s.tips)}</td>
      <td class="amount">${pct(s.tip_pct)}</td>
      <td class="amount">${fmt(s.gratuity)}</td>
    </tr>`).join('');
  }

  window.showServerDetail=function(name){
    if(!cachedData)return;
    const s=cachedData.servers.find(x=>x.server===name);
    if(!s)return;
    $('detailName').textContent=name+' — Detail';
    // DOW breakdown
    const dows=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    const dowData=s.dow||[];
    $('detailDow').innerHTML=dows.map(d=>{
      const row=dowData.find(x=>x.dow===d)||{revenue:0,orders:0,avg_check:0};
      return `<tr><td>${d}</td><td class="amount">${fmt(row.revenue)}</td><td class="amount">${row.orders||0}</td><td class="amount">${fmtD(row.avg_check)}</td></tr>`;
    }).join('');
    // Hourly breakdown
    const hourData=s.hourly||[];
    $('detailHourly').innerHTML=hourData.map(h=>`<tr><td>${String(h.hour).padStart(2,'0')}:00</td><td class="amount">${fmt(h.revenue)}</td><td class="amount">${h.orders||0}</td></tr>`).join('')||'<tr><td colspan="3" style="color:#666">No hourly data</td></tr>';
    $('detailPanel').classList.add('active');
    $('detailPanel').scrollIntoView({behavior:'smooth',block:'start'});
  };

  window.loadServerPerf=loadServerPerf;
  loadServerPerf();
})();
</script>
</body>
</html>'''



def _kitchen_speed_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Kitchen Speed</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e7eb;min-height:100vh}
.header{background:linear-gradient(135deg,#d97706,#f59e0b,#fbbf24);padding:24px 32px;color:#fff}
.header h1{font-size:1.5rem;font-weight:800;letter-spacing:-0.5px}.header .subtitle{font-size:0.85rem;opacity:0.9;margin-top:4px}
.nav-bar{background:#1a1a1a;border-bottom:1px solid #333;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#9ca3af;transition:all 0.15s}
.nav-bar a:hover{background:#222;color:#fff}
.nav-bar a.active{background:#f59e0b;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.filter-bar{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:16px 20px;margin-bottom:24px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.filter-bar label{font-size:0.82rem;color:#9ca3af;font-weight:600}
.filter-bar input[type="date"]{background:#111;border:1px solid #444;color:#e5e7eb;padding:8px 12px;border-radius:8px;font-size:0.85rem}
.filter-bar .btn{background:linear-gradient(135deg,#d97706,#f59e0b);color:#fff;border:none;padding:8px 24px;border-radius:8px;font-weight:700;font-size:0.85rem;cursor:pointer}
.filter-bar .btn:hover{opacity:0.9}
.kpi-row{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.kpi-row{grid-template-columns:repeat(2,1fr)}}
.kpi{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#9ca3af;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}
.kpi .value{font-size:1.6rem;font-weight:800;color:#f59e0b;margin-top:4px;font-family:"SF Mono",monospace}
.section{background:#1e1e1e;border:1px solid #333;border-radius:12px;margin-bottom:24px;overflow:hidden}
.section-title{font-size:1.1rem;font-weight:700;padding:16px 20px;color:#fff;border-bottom:1px solid #333}
.section-body{padding:0;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem}
th{text-align:left;padding:10px 14px;background:#1a1a1a;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:0.7rem;letter-spacing:0.5px;border-bottom:1px solid #333;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #222;color:#ccc;vertical-align:middle}
tr:hover td{background:#1a1a1a}
.amount{font-family:"SF Mono",monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
.rank-num{font-weight:700;color:#f59e0b}
.bar-cell{display:flex;align-items:center;gap:10px}
.bar-track{flex:1;height:18px;background:#222;border-radius:4px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;min-width:2px;background:linear-gradient(90deg,#d97706,#f59e0b)}
.bar-fill.fast{background:linear-gradient(90deg,#059669,#10b981)}
.bar-fill.slow{background:linear-gradient(90deg,#dc2626,#ef4444)}
.bar-value{font-family:"SF Mono",monospace;font-size:0.8rem;font-weight:600;min-width:70px;text-align:right;color:#f59e0b}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600}
.badge-fast{background:#064e3b;color:#34d399}
.badge-avg{background:#422006;color:#fbbf24}
.badge-backed{background:#7c2d12;color:#fdba74}
.badge-slow{background:#450a0a;color:#fca5a5}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#1e1e1e;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.3);text-align:center;color:#ccc}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #f59e0b;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Kitchen Speed</h1>
  <div class="subtitle">Station performance, cook leaderboard, and fulfillment tracking</div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen" class="active">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label><input type="date" id="startDate">
    <label>To</label><input type="date" id="endDate">
    <button class="btn" onclick="window.loadKitchen()">Analyze</button>
  </div>

  <div id="kpiRow" class="kpi-row"></div>

  <div class="section">
    <div class="section-title">Station Performance</div>
    <div class="section-body"><table><thead><tr>
      <th>Station</th><th class="text-right">Tickets</th><th>Avg Time</th><th class="text-right">Median</th><th class="text-right">Fastest</th><th class="text-right">Slowest</th><th class="text-right">Fulfillment %</th>
    </tr></thead><tbody id="stationTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Hourly Speed Profile</div>
    <div class="section-body"><table><thead><tr>
      <th>Hour</th><th class="text-right">Ticket Volume</th><th>Avg Time</th>
    </tr></thead><tbody id="hourlyTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Cook Leaderboard</div>
    <div class="section-body"><table><thead><tr>
      <th>#</th><th>Cook</th><th class="text-right">Tickets Fulfilled</th><th>Avg Time</th><th class="text-right">Fastest</th>
    </tr></thead><tbody id="cookTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Weekly Trend</div>
    <div class="section-body"><table><thead><tr>
      <th>Week Starting</th><th class="text-right">Tickets</th><th class="text-right">Fulfilled</th><th>Avg Time</th>
    </tr></thead><tbody id="weeklyTable"></tbody></table></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Analyzing kitchen data...</div>
</div>

<script>
(function(){
  const $=id=>document.getElementById(id);

  function fmtTime(sec){
    if(sec==null||sec<=0)return '—';
    const m=Math.floor(sec/60);
    const s=Math.round(sec%60);
    if(m>0)return m+'m '+s+'s';
    return s+'s';
  }

  function timeBadge(sec){
    if(sec==null)return '';
    if(sec<600)return '<span class="badge badge-fast">Fast</span>';
    if(sec<900)return '<span class="badge badge-avg">Average</span>';
    if(sec<1200)return '<span class="badge badge-backed">Backed Up</span>';
    return '<span class="badge badge-slow">Slow</span>';
  }

  const today=new Date();
  const start=new Date(today);start.setMonth(start.getMonth()-3);
  $('startDate').value=start.toISOString().slice(0,10);
  $('endDate').value=today.toISOString().slice(0,10);

  async function loadKitchen(){
    $('loadingOverlay').classList.add('active');
    try{
      const res=await fetch('/api/kitchen-speed',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:$('startDate').value,end_date:$('endDate').value})});
      const data=await res.json();
      if(!res.ok)throw new Error(data.error||'API error');
      renderKPIs(data.kpis);
      renderStations(data.stations);
      renderHourly(data.hourly);
      renderCooks(data.cooks);
      renderWeekly(data.weekly);
    }catch(e){alert('Error: '+e.message)}
    finally{$('loadingOverlay').classList.remove('active')}
  }

  function renderKPIs(k){
    const items=[
      ['Total Tickets',(k.total_tickets||0).toLocaleString()],
      ['Avg Fulfillment',fmtTime(k.avg_fulfillment_sec)],
      ['Fastest Station',k.fastest_station||'—'],
      ['Slowest Station',k.slowest_station||'—'],
      ['Fulfillment Rate',(k.fulfillment_rate||0).toFixed(1)+'%']
    ];
    $('kpiRow').innerHTML=items.map(([l,v])=>`<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');
  }

  function renderStations(stations){
    const maxAvg=Math.max(...stations.map(s=>s.avg_sec||0),1);
    $('stationTable').innerHTML=stations.map(s=>{
      const pctW=((s.avg_sec||0)/maxAvg*100).toFixed(1);
      return `<tr>
        <td><strong>${s.station}</strong></td>
        <td class="amount">${(s.tickets||0).toLocaleString()}</td>
        <td><div class="bar-cell"><div class="bar-track"><div class="bar-fill" style="width:${pctW}%"></div></div><div class="bar-value">${fmtTime(s.avg_sec)}</div></div></td>
        <td class="amount">${fmtTime(s.median_sec)}</td>
        <td class="amount">${fmtTime(s.min_sec)}</td>
        <td class="amount">${fmtTime(s.max_sec)}</td>
        <td class="amount">${(s.fulfillment_pct||0).toFixed(1)}% ${timeBadge(s.avg_sec)}</td>
      </tr>`;
    }).join('')||'<tr><td colspan="7" style="text-align:center;color:#666;padding:20px">No station data</td></tr>';
  }

  function renderHourly(hourly){
    const maxVol=Math.max(...hourly.map(h=>h.tickets||0),1);
    $('hourlyTable').innerHTML=hourly.map(h=>`<tr>
      <td>${String(h.hour).padStart(2,'0')}:00</td>
      <td class="amount">${(h.tickets||0).toLocaleString()}</td>
      <td><div class="bar-cell"><div class="bar-track"><div class="bar-fill" style="width:${((h.avg_sec||0)/Math.max(...hourly.map(x=>x.avg_sec||0),1)*100).toFixed(1)}%"></div></div><div class="bar-value">${fmtTime(h.avg_sec)} ${timeBadge(h.avg_sec)}</div></div></td>
    </tr>`).join('')||'<tr><td colspan="3" style="text-align:center;color:#666;padding:20px">No hourly data</td></tr>';
  }

  function renderCooks(cooks){
    $('cookTable').innerHTML=cooks.map((c,i)=>`<tr>
      <td class="rank-num">${i+1}</td>
      <td><strong>${c.cook}</strong></td>
      <td class="amount">${(c.tickets||0).toLocaleString()}</td>
      <td>${fmtTime(c.avg_sec)} ${timeBadge(c.avg_sec)}</td>
      <td class="amount">${fmtTime(c.min_sec)}</td>
    </tr>`).join('')||'<tr><td colspan="5" style="text-align:center;color:#666;padding:20px">No cook data</td></tr>';
  }

  function renderWeekly(weekly){
    $('weeklyTable').innerHTML=weekly.map(w=>`<tr>
      <td>${w.week}</td>
      <td class="amount">${(w.tickets||0).toLocaleString()}</td>
      <td class="amount">${(w.fulfilled||0).toLocaleString()}</td>
      <td>${fmtTime(w.avg_sec)} ${timeBadge(w.avg_sec)}</td>
    </tr>`).join('')||'<tr><td colspan="4" style="text-align:center;color:#666;padding:20px">No weekly data</td></tr>';
  }

  window.loadKitchen=loadKitchen;
  loadKitchen();
})();
</script>
</body>
</html>'''



def _labor_dashboard_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Labor Analysis</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e7eb;min-height:100vh}
.header{background:linear-gradient(135deg,#1d4ed8,#3b82f6,#60a5fa);padding:24px 32px;color:#fff}
.header h1{font-size:1.5rem;font-weight:800;letter-spacing:-0.5px}.header .subtitle{font-size:0.85rem;opacity:0.9;margin-top:4px}
.nav-bar{background:#1a1a1a;border-bottom:1px solid #333;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#9ca3af;transition:all 0.15s}
.nav-bar a:hover{background:#222;color:#fff}
.nav-bar a.active{background:#3b82f6;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.filter-bar{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:16px 20px;margin-bottom:24px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.filter-bar label{font-size:0.82rem;color:#9ca3af;font-weight:600}
.filter-bar input[type="date"]{background:#111;border:1px solid #444;color:#e5e7eb;padding:8px 12px;border-radius:8px;font-size:0.85rem}
.filter-bar .btn{background:linear-gradient(135deg,#1d4ed8,#3b82f6);color:#fff;border:none;padding:8px 24px;border-radius:8px;font-weight:700;font-size:0.85rem;cursor:pointer}
.filter-bar .btn:hover{opacity:0.9}
.kpi-row{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.kpi-row{grid-template-columns:repeat(2,1fr)}}
.kpi{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#9ca3af;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}
.kpi .value{font-size:1.6rem;font-weight:800;color:#3b82f6;margin-top:4px;font-family:"SF Mono",monospace}
.section{background:#1e1e1e;border:1px solid #333;border-radius:12px;margin-bottom:24px;overflow:hidden}
.section-title{font-size:1.1rem;font-weight:700;padding:16px 20px;color:#fff;border-bottom:1px solid #333}
.section-body{padding:0;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem}
th{text-align:left;padding:10px 14px;background:#1a1a1a;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:0.7rem;letter-spacing:0.5px;border-bottom:1px solid #333;white-space:nowrap}
td{padding:10px 14px;border-bottom:1px solid #222;color:#ccc;vertical-align:middle}
tr:hover td{background:#1a1a1a}
.amount{font-family:"SF Mono",monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
.bar-cell{display:flex;align-items:center;gap:10px}
.bar-track{flex:1;height:18px;background:#222;border-radius:4px;overflow:hidden;position:relative}
.bar-fill{height:100%;border-radius:4px;min-width:2px}
.bar-fill.green{background:linear-gradient(90deg,#059669,#10b981)}
.bar-fill.amber{background:linear-gradient(90deg,#d97706,#f59e0b)}
.bar-fill.red{background:linear-gradient(90deg,#dc2626,#ef4444)}
.bar-target{position:absolute;top:0;bottom:0;width:2px;background:#fff;opacity:0.5}
.bar-value{font-family:"SF Mono",monospace;font-size:0.8rem;font-weight:600;min-width:55px;text-align:right}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600}
.badge-lean{background:#064e3b;color:#34d399}
.badge-target{background:#422006;color:#fbbf24}
.badge-high{background:#450a0a;color:#fca5a5}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#1e1e1e;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.3);text-align:center;color:#ccc}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #3b82f6;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Labor Analysis</h1>
  <div class="subtitle">Weekly labor cost tracking, true labor %, and vendor breakdown</div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor" class="active">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label><input type="date" id="startDate">
    <label>To</label><input type="date" id="endDate">
    <button class="btn" onclick="window.loadLabor()">Analyze</button>
  </div>

  <div id="kpiRow" class="kpi-row"></div>

  <div class="section">
    <div class="section-title">Weekly Labor Trend</div>
    <div class="section-body"><table><thead><tr>
      <th>Week</th><th class="text-right">Revenue</th><th class="text-right">Gross Labor</th><th class="text-right">True Labor</th><th>Labor %</th><th class="text-right">Pass-Through</th><th class="text-right">Orders</th>
    </tr></thead><tbody id="weeklyTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Monthly Summary</div>
    <div class="section-body"><table><thead><tr>
      <th>Month</th><th class="text-right">Revenue</th><th class="text-right">COGS</th><th class="text-right">True Labor</th><th>Labor %</th><th>Prime Cost %</th>
    </tr></thead><tbody id="monthlyTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Labor Vendor Breakdown</div>
    <div class="section-body"><table><thead><tr>
      <th>Vendor</th><th class="text-right">Total Paid</th><th class="text-right">Transactions</th>
    </tr></thead><tbody id="vendorTable"></tbody></table></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Analyzing labor data...</div>
</div>

<script>
(function(){
  const $=id=>document.getElementById(id);
  const fmt=v=>'$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  const pct=v=>(v||0).toFixed(1)+'%';

  // Default 6 months back
  const today=new Date();
  const start=new Date(today);start.setMonth(start.getMonth()-6);
  $('startDate').value=start.toISOString().slice(0,10);
  $('endDate').value=today.toISOString().slice(0,10);

  function laborBadge(p){
    if(p==null)return '';
    if(p<25)return '<span class="badge badge-lean">Lean</span>';
    if(p<=35)return '<span class="badge badge-target">Target</span>';
    return '<span class="badge badge-high">High</span>';
  }
  function laborBarClass(p){
    if(p<25)return 'green';
    if(p<=35)return 'amber';
    return 'red';
  }

  async function loadLabor(){
    $('loadingOverlay').classList.add('active');
    try{
      const res=await fetch('/api/labor-analysis',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:$('startDate').value,end_date:$('endDate').value})});
      const data=await res.json();
      if(!res.ok)throw new Error(data.error||'API error');
      renderKPIs(data.kpis);
      renderWeekly(data.weekly);
      renderMonthly(data.monthly);
      renderVendors(data.by_vendor);
    }catch(e){alert('Error: '+e.message)}
    finally{$('loadingOverlay').classList.remove('active')}
  }

  function renderKPIs(k){
    const items=[
      ['Avg Weekly Labor',fmt(k.avg_weekly_labor)],
      ['Avg Labor %',pct(k.avg_labor_pct)],
      ['Best Week %',pct(k.best_week_pct)],
      ['Worst Week %',pct(k.worst_week_pct)],
      ['Total True Labor',fmt(k.total_labor_true)]
    ];
    $('kpiRow').innerHTML=items.map(([l,v])=>`<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');
  }

  function renderWeekly(weekly){
    const maxPct=Math.max(...weekly.map(w=>w.labor_pct||0),40);
    $('weeklyTable').innerHTML=weekly.map(w=>{
      const p=w.labor_pct||0;
      const targetPos=(30/maxPct*100).toFixed(1);
      return `<tr>
        <td>${w.week_start}</td>
        <td class="amount">${fmt(w.revenue)}</td>
        <td class="amount">${fmt(w.labor_gross)}</td>
        <td class="amount">${fmt(w.labor_true)}</td>
        <td><div class="bar-cell"><div class="bar-track"><div class="bar-fill ${laborBarClass(p)}" style="width:${(p/maxPct*100).toFixed(1)}%"></div><div class="bar-target" style="left:${targetPos}%"></div></div><div class="bar-value">${pct(p)} ${laborBadge(p)}</div></div></td>
        <td class="amount">${fmt(w.pass_through)}</td>
        <td class="amount">${(w.order_count||0).toLocaleString()}</td>
      </tr>`;
    }).join('')||'<tr><td colspan="7" style="text-align:center;color:#666;padding:20px">No data</td></tr>';
  }

  function renderMonthly(monthly){
    $('monthlyTable').innerHTML=monthly.map(m=>{
      const lp=m.labor_pct||0;
      const pp=m.prime_cost_pct||0;
      return `<tr>
        <td><strong>${m.month}</strong></td>
        <td class="amount">${fmt(m.revenue)}</td>
        <td class="amount">${fmt(m.cogs)}</td>
        <td class="amount">${fmt(m.labor_true)}</td>
        <td class="amount">${pct(lp)} ${laborBadge(lp)}</td>
        <td class="amount">${pct(pp)} ${pp>65?'<span class="badge badge-high">High</span>':pp>55?'<span class="badge badge-target">OK</span>':'<span class="badge badge-lean">Good</span>'}</td>
      </tr>`;
    }).join('')||'<tr><td colspan="6" style="text-align:center;color:#666;padding:20px">No data</td></tr>';
  }

  function renderVendors(vendors){
    $('vendorTable').innerHTML=vendors.map(v=>`<tr>
      <td><strong>${v.vendor}</strong></td>
      <td class="amount">${fmt(v.total)}</td>
      <td class="amount">${v.txn_count}</td>
    </tr>`).join('')||'<tr><td colspan="3" style="text-align:center;color:#666;padding:20px">No vendor data</td></tr>';
  }

  window.loadLabor=loadLabor;
  loadLabor();
})();
</script>
</body>
</html>'''



def _menu_engineering_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Menu Engineering</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e7eb;min-height:100vh}
.header{background:linear-gradient(135deg,#7c3aed,#8b5cf6,#a78bfa);padding:24px 32px;color:#fff}
.header h1{font-size:1.5rem;font-weight:800;letter-spacing:-0.5px}.header .subtitle{font-size:0.85rem;opacity:0.9;margin-top:4px}
.nav-bar{background:#1a1a1a;border-bottom:1px solid #333;padding:8px 32px;display:flex;gap:8px;flex-wrap:wrap}
.nav-bar a{text-decoration:none;padding:8px 20px;border-radius:9999px;font-size:0.85rem;font-weight:600;color:#9ca3af;transition:all 0.15s}
.nav-bar a:hover{background:#222;color:#fff}
.nav-bar a.active{background:#8b5cf6;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:24px}
.filter-bar{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:16px 20px;margin-bottom:24px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.filter-bar label{font-size:0.82rem;color:#9ca3af;font-weight:600}
.filter-bar input[type="date"]{background:#111;border:1px solid #444;color:#e5e7eb;padding:8px 12px;border-radius:8px;font-size:0.85rem}
.filter-bar .btn{background:linear-gradient(135deg,#7c3aed,#8b5cf6);color:#fff;border:none;padding:8px 24px;border-radius:8px;font-weight:700;font-size:0.85rem;cursor:pointer}
.filter-bar .btn:hover{opacity:0.9}
.kpi-row{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.kpi-row{grid-template-columns:repeat(2,1fr)}}
.kpi{background:#1e1e1e;border:1px solid #333;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#9ca3af;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}
.kpi .value{font-size:1.6rem;font-weight:800;color:#8b5cf6;margin-top:4px;font-family:"SF Mono",monospace}
.kpi .sub{font-size:0.72rem;color:#666;margin-top:2px}
.matrix-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:768px){.matrix-grid{grid-template-columns:1fr}}
.matrix-card{border-radius:12px;padding:20px;border:1px solid #333}
.matrix-card h3{font-size:1rem;margin-bottom:4px}
.matrix-card .count{font-size:1.8rem;font-weight:800;font-family:"SF Mono",monospace}
.matrix-card .advice{font-size:0.78rem;margin-top:6px;opacity:0.8}
.matrix-card.star{background:#1a1500;border-color:#854d0e;color:#fbbf24}.matrix-card.star .count{color:#fbbf24}
.matrix-card.plowhorse{background:#001a0e;border-color:#065f46;color:#34d399}.matrix-card.plowhorse .count{color:#34d399}
.matrix-card.puzzle{background:#0a001a;border-color:#3730a3;color:#818cf8}.matrix-card.puzzle .count{color:#818cf8}
.matrix-card.dog{background:#1a0000;border-color:#991b1b;color:#fca5a5}.matrix-card.dog .count{color:#fca5a5}
.filter-btns{display:flex;gap:8px;margin-bottom:24px;flex-wrap:wrap}
.filter-btns button{background:#1e1e1e;border:1px solid #444;color:#9ca3af;padding:6px 16px;border-radius:8px;font-size:0.8rem;font-weight:600;cursor:pointer;transition:all 0.15s}
.filter-btns button:hover{border-color:#8b5cf6;color:#fff}
.filter-btns button.active{background:#8b5cf6;border-color:#8b5cf6;color:#fff}
.section{background:#1e1e1e;border:1px solid #333;border-radius:12px;margin-bottom:24px;overflow:hidden}
.section-title{font-size:1.1rem;font-weight:700;padding:16px 20px;color:#fff;border-bottom:1px solid #333}
.section-body{padding:0;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:0.82rem}
th{text-align:left;padding:10px 14px;background:#1a1a1a;color:#9ca3af;font-weight:600;text-transform:uppercase;font-size:0.7rem;letter-spacing:0.5px;border-bottom:1px solid #333;white-space:nowrap;cursor:pointer}
th:hover{color:#fff}
th.sorted-asc::after{content:' \\25B2';font-size:0.6rem}
th.sorted-desc::after{content:' \\25BC';font-size:0.6rem}
td{padding:10px 14px;border-bottom:1px solid #222;color:#ccc;vertical-align:middle}
tr:hover td{background:#1a1a1a}
.amount{font-family:"SF Mono",monospace;text-align:right;white-space:nowrap}
.text-right{text-align:right}
.badge-class{display:inline-block;padding:2px 10px;border-radius:4px;font-size:0.72rem;font-weight:700}
.badge-star{background:#422006;color:#fbbf24}
.badge-plowhorse{background:#064e3b;color:#34d399}
.badge-puzzle{background:#1e1b4b;color:#818cf8}
.badge-dog{background:#450a0a;color:#fca5a5}
.hidden{display:none}
.loading-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9998;align-items:center;justify-content:center}
.loading-overlay.active{display:flex}
.loading-box{background:#1e1e1e;padding:24px 32px;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,0.3);text-align:center;color:#ccc}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid #8b5cf6;border-top-color:transparent;border-radius:50%;animation:spin 0.6s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Menu Engineering</h1>
  <div class="subtitle">Item classification matrix &mdash; Stars, Plowhorses, Puzzles &amp; Dogs</div>
</div>
<div class="nav-bar">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L Summary</a>
  <a href="/analysis">Comprehensive Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng" class="active">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>

<div class="container">
  <div class="filter-bar">
    <label>From</label><input type="date" id="startDate">
    <label>To</label><input type="date" id="endDate">
    <button class="btn" onclick="window.loadMenuEng()">Analyze</button>
  </div>

  <div id="kpiRow" class="kpi-row"></div>

  <div id="matrixGrid" class="matrix-grid"></div>

  <div style="display:flex;align-items:center;gap:16px;margin-bottom:24px;flex-wrap:wrap">
    <div id="filterBtns" class="filter-btns" style="margin-bottom:0"></div>
    <select id="catFilter" onchange="window.filterCategory(this.value)" style="background:#1e1e1e;border:1px solid #444;color:#9ca3af;padding:6px 16px;border-radius:8px;font-size:0.8rem;font-weight:600;cursor:pointer">
      <option value="all">All Categories</option>
      <option value="Food">Food</option>
      <option value="Liquor">Liquor</option>
      <option value="NA Beverage">NA Beverage</option>
    </select>
  </div>

  <div class="section">
    <div class="section-title">Item Classification</div>
    <div class="section-body"><table><thead><tr>
      <th data-col="menu_item">Item</th><th data-col="sales_category">Category</th><th data-col="qty_sold" class="text-right">Qty Sold</th><th data-col="net_revenue" class="text-right">Revenue</th><th data-col="avg_price" class="text-right">Avg Price</th><th data-col="popularity_index" class="text-right">Pop. Idx</th><th data-col="profitability_index" class="text-right">Prof. Idx</th><th data-col="classification">Class</th>
    </tr></thead><tbody id="itemTable"></tbody></table></div>
  </div>

  <div class="section">
    <div class="section-title">Category Breakdown</div>
    <div class="section-body"><table><thead><tr>
      <th>Category</th><th class="text-right">Revenue</th><th class="text-right">Qty</th><th class="text-right">Items</th><th class="text-right">Stars</th><th class="text-right">Plowhorses</th><th class="text-right">Puzzles</th><th class="text-right">Dogs</th>
    </tr></thead><tbody id="catTable"></tbody></table></div>
  </div>
</div>

<div class="loading-overlay" id="loadingOverlay">
  <div class="loading-box"><span class="spinner"></span> Analyzing menu items...</div>
</div>

<script>
(function(){
  const $=id=>document.getElementById(id);
  const fmt=v=>'$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  const fmtD=v=>'$'+Number(v||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});

  const today=new Date();
  const start=new Date(today);start.setMonth(start.getMonth()-3);
  $('startDate').value=start.toISOString().slice(0,10);
  $('endDate').value=today.toISOString().slice(0,10);

  let allItems=[];
  let currentFilter='all';
  let currentCatFilter='all';
  let sortCol='net_revenue';
  let sortDir='desc';

  function classBadge(c){
    const m={Star:'badge-star',Plowhorse:'badge-plowhorse',Puzzle:'badge-puzzle',Dog:'badge-dog'};
    const icons={Star:'\\u2B50',Plowhorse:'\\uD83D\\uDC34',Puzzle:'\\uD83E\\uDDE9',Dog:'\\uD83D\\uDC15'};
    return `<span class="badge-class ${m[c]||''}">${icons[c]||''} ${c}</span>`;
  }

  async function loadMenuEng(){
    $('loadingOverlay').classList.add('active');
    try{
      const res=await fetch('/api/menu-engineering',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:$('startDate').value,end_date:$('endDate').value})});
      const data=await res.json();
      if(!res.ok)throw new Error(data.error||'API error');
      allItems=data.items||[];
      renderKPIs(data.kpis);
      renderMatrix(data.kpis);
      renderFilterBtns(data.kpis);
      renderItems();
      renderCategories(data.categories);
    }catch(e){alert('Error: '+e.message)}
    finally{$('loadingOverlay').classList.remove('active')}
  }

  function renderKPIs(k){
    const items=[
      ['Total Items',k.total_items,''],
      ['Stars',k.stars_count,k.stars_revenue_pct+'% rev'],
      ['Plowhorses',k.plowhorses_count,k.plowhorses_revenue_pct+'% rev'],
      ['Puzzles',k.puzzles_count,k.puzzles_revenue_pct+'% rev'],
      ['Dogs',k.dogs_count,k.dogs_revenue_pct+'% rev']
    ];
    $('kpiRow').innerHTML=items.map(([l,v,s])=>`<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div>${s?'<div class="sub">'+s+'</div>':''}</div>`).join('');
  }

  function renderMatrix(k){
    const cards=[
      {cls:'star',title:'\\u2B50 Stars',count:k.stars_count,advice:'High volume + High revenue/item. Keep promoting these winners.'},
      {cls:'puzzle',title:'\\uD83E\\uDDE9 Puzzles',count:k.puzzles_count,advice:'Low volume + High revenue/item. Market more to boost sales.'},
      {cls:'plowhorse',title:'\\uD83D\\uDC34 Plowhorses',count:k.plowhorses_count,advice:'High volume + Low revenue/item. Increase price or pair with upsells.'},
      {cls:'dog',title:'\\uD83D\\uDC15 Dogs',count:k.dogs_count,advice:'Low volume + Low revenue/item. Consider removing or reinventing.'}
    ];
    $('matrixGrid').innerHTML=cards.map(c=>`<div class="matrix-card ${c.cls}"><h3>${c.title}</h3><div class="count">${c.count}</div><div class="advice">${c.advice}</div></div>`).join('');
  }

  function renderFilterBtns(k){
    const btns=[
      {key:'all',label:'All ('+k.total_items+')'},
      {key:'Star',label:'\\u2B50 Stars ('+k.stars_count+')'},
      {key:'Plowhorse',label:'\\uD83D\\uDC34 Plowhorses ('+k.plowhorses_count+')'},
      {key:'Puzzle',label:'\\uD83E\\uDDE9 Puzzles ('+k.puzzles_count+')'},
      {key:'Dog',label:'\\uD83D\\uDC15 Dogs ('+k.dogs_count+')'}
    ];
    $('filterBtns').innerHTML=btns.map(b=>`<button class="${currentFilter===b.key?'active':''}" onclick="window.filterItems('${b.key}')">${b.label}</button>`).join('');
  }

  window.filterItems=function(key){
    currentFilter=key;
    document.querySelectorAll('.filter-btns button').forEach(b=>b.classList.remove('active'));
    event.target.classList.add('active');
    renderItems();
  };

  window.filterCategory=function(val){
    currentCatFilter=val;
    renderItems();
  };

  // Column sorting
  document.querySelector('.section-body table thead').addEventListener('click',function(e){
    const th=e.target.closest('th');
    if(!th||!th.dataset.col)return;
    const col=th.dataset.col;
    if(sortCol===col)sortDir=sortDir==='asc'?'desc':'asc';
    else{sortCol=col;sortDir='desc';}
    document.querySelectorAll('th').forEach(t=>{t.classList.remove('sorted-asc','sorted-desc')});
    th.classList.add(sortDir==='asc'?'sorted-asc':'sorted-desc');
    renderItems();
  });

  function renderItems(){
    let items=allItems.filter(i=>(currentFilter==='all'||i.classification===currentFilter)&&(currentCatFilter==='all'||i.sales_category===currentCatFilter));
    items.sort((a,b)=>{
      let va=a[sortCol],vb=b[sortCol];
      if(typeof va==='string')return sortDir==='asc'?va.localeCompare(vb):vb.localeCompare(va);
      return sortDir==='asc'?(va-vb):(vb-va);
    });
    $('itemTable').innerHTML=items.map(i=>`<tr>
      <td><strong>${i.menu_item}</strong></td>
      <td>${i.sales_category}</td>
      <td class="amount">${(i.qty_sold||0).toLocaleString()}</td>
      <td class="amount">${fmt(i.net_revenue)}</td>
      <td class="amount">${fmtD(i.avg_price)}</td>
      <td class="amount">${(i.popularity_index||0).toFixed(2)}</td>
      <td class="amount">${(i.profitability_index||0).toFixed(2)}</td>
      <td>${classBadge(i.classification)}</td>
    </tr>`).join('')||'<tr><td colspan="8" style="text-align:center;color:#666;padding:20px">No items</td></tr>';
  }

  function renderCategories(cats){
    $('catTable').innerHTML=(cats||[]).map(c=>`<tr>
      <td><strong>${c.category}</strong></td>
      <td class="amount">${fmt(c.revenue)}</td>
      <td class="amount">${(c.qty||0).toLocaleString()}</td>
      <td class="amount">${c.item_count}</td>
      <td class="amount">${c.stars||0}</td>
      <td class="amount">${c.plowhorses||0}</td>
      <td class="amount">${c.puzzles||0}</td>
      <td class="amount">${c.dogs||0}</td>
    </tr>`).join('')||'<tr><td colspan="8" style="text-align:center;color:#666;padding:20px">No data</td></tr>';
  }

  window.loadMenuEng=loadMenuEng;
  loadMenuEng();
})();
</script>
</body>
</html>'''



def _kpi_benchmarks_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 KPI Benchmarks</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e5e7eb;min-height:100vh}
.header{background:linear-gradient(135deg,#6366f1,#818cf8,#a5b4fc);padding:32px 40px;border-bottom:3px solid #4f46e5}
.header h1{font-size:28px;font-weight:700;color:#fff}
.header p{color:rgba(255,255,255,.85);margin-top:4px;font-size:14px}
.nav{display:flex;gap:0;background:#1a1a2e;border-bottom:1px solid #333;overflow-x:auto}
.nav a{padding:10px 16px;color:#9ca3af;text-decoration:none;font-size:13px;white-space:nowrap;border-bottom:2px solid transparent;transition:all .2s}
.nav a:hover{color:#a5b4fc;background:rgba(99,102,241,.1)}
.nav a.active{color:#a5b4fc;border-bottom-color:#818cf8;background:rgba(99,102,241,.15)}
.container{max-width:1400px;margin:0 auto;padding:24px}
.toggle-bar{display:flex;gap:8px;margin-bottom:20px;align-items:center}
.toggle-btn{padding:8px 20px;border:1px solid #4f46e5;background:transparent;color:#a5b4fc;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;transition:all .2s}
.toggle-btn.active{background:#6366f1;color:#fff;border-color:#6366f1}
.toggle-btn:hover{background:rgba(99,102,241,.3)}
.period-label{color:#9ca3af;font-size:13px;margin-left:12px}
.prior-label{color:#6b7280;font-size:12px;margin-left:8px}
.banner{display:flex;gap:16px;align-items:center;padding:16px 24px;border-radius:10px;margin-bottom:24px;background:#1e1b4b;border:1px solid #312e81}
.banner .dot{width:12px;height:12px;border-radius:50%;display:inline-block}
.banner .stat{font-size:15px;font-weight:600;color:#e5e7eb}
.banner .stat span{margin-left:4px;font-weight:400;color:#9ca3af;font-size:13px}
.stale-warning{background:#78350f;border:1px solid #92400e;padding:14px 20px;border-radius:8px;margin-bottom:20px;color:#fbbf24;font-size:13px}
.section-title{font-size:18px;font-weight:700;color:#c7d2fe;margin:28px 0 16px;padding-bottom:8px;border-bottom:1px solid #312e81}
.grid-4{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px}
.card{background:#1e1b4b;border:1px solid #312e81;border-radius:10px;padding:18px;transition:border-color .2s}
.card:hover{border-color:#6366f1}
.card .card-label{font-size:12px;color:#9ca3af;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center}
.card .card-value{font-size:28px;font-weight:700;line-height:1.2}
.card .card-delta{font-size:13px;margin-top:6px}
.card .card-bench{font-size:11px;color:#6b7280;margin-top:4px}
.dot-good{background:#22c55e}
.dot-watch{background:#f59e0b}
.dot-critical{background:#ef4444}
.clr-good{color:#22c55e}
.clr-watch{color:#f59e0b}
.clr-critical{color:#ef4444}
.gauge-row{display:flex;align-items:center;gap:12px;margin-bottom:14px;padding:10px 16px;background:#1e1b4b;border:1px solid #312e81;border-radius:8px}
.gauge-label{width:130px;font-size:13px;font-weight:600;color:#c7d2fe;flex-shrink:0}
.gauge-wrap{flex:1;height:14px;background:#0f0d2e;border-radius:7px;position:relative;overflow:hidden}
.gauge-fill{height:100%;border-radius:7px;transition:width .6s ease}
.gauge-val{width:60px;text-align:right;font-size:14px;font-weight:700;flex-shrink:0}
.gauge-delta{width:80px;text-align:right;font-size:12px;flex-shrink:0}
.gauge-status{width:12px;height:12px;border-radius:50%;flex-shrink:0}
.sparkline-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px}
.spark-card{background:#1e1b4b;border:1px solid #312e81;border-radius:8px;padding:14px}
.spark-card .spark-label{font-size:12px;color:#9ca3af;margin-bottom:8px}
.spark-card .spark-val{font-size:18px;font-weight:700;color:#c7d2fe;margin-bottom:8px}
.sparkline{display:flex;align-items:flex-end;gap:3px;height:40px;position:relative}
.sparkline .bar{flex:1;border-radius:2px 2px 0 0;min-width:8px;transition:height .3s;opacity:.8}
.sparkline .bar:last-child{opacity:1}
.sparkline .bench-line{position:absolute;left:0;right:0;border-top:2px dashed rgba(255,255,255,.2)}
.legend{margin-top:32px}
.legend summary{cursor:pointer;color:#818cf8;font-size:14px;font-weight:600;padding:8px 0}
.legend table{width:100%;border-collapse:collapse;margin-top:12px;font-size:12px}
.legend th{text-align:left;padding:8px 10px;color:#9ca3af;border-bottom:1px solid #312e81;font-weight:600}
.legend td{padding:7px 10px;border-bottom:1px solid #1e1b4b;color:#d1d5db}
.legend .src{color:#6b7280;font-style:italic}
.loading{text-align:center;padding:60px;color:#9ca3af;font-size:15px}
.filter-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.filter-row label{font-size:12px;color:#9ca3af}
.filter-row input[type="date"]{background:#1e1b4b;border:1px solid #312e81;color:#e5e7eb;padding:6px 10px;border-radius:6px;font-size:13px}
.filter-row input[type="date"]::-webkit-calendar-picker-indicator{filter:invert(.7)}
.analyze-btn{padding:8px 20px;background:#6366f1;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;transition:background .2s}
.analyze-btn:hover{background:#4f46e5}
.analyze-btn:disabled{opacity:.5;cursor:not-allowed}
.insight-card{background:#1e1b4b;border:1px solid #312e81;border-radius:10px;padding:16px 18px;margin-bottom:10px;display:flex;gap:14px;align-items:flex-start}
.insight-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;margin-top:5px}
.insight-label{font-size:13px;font-weight:700;color:#c7d2fe;margin-bottom:4px}
.insight-text{font-size:13px;color:#d1d5db;line-height:1.5}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 KPI Benchmarks</h1>
  <p>Performance scorecard with industry benchmarks &mdash; MTD &amp; YTD</p>
</div>
<div class="nav">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L</a>
  <a href="/analysis">Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks" class="active">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi">Event ROI</a>
</div>
<div class="container">
  <div class="toggle-bar">
    <button class="toggle-btn active" id="btnMTD" onclick="toggleView('mtd')">MTD</button>
    <button class="toggle-btn" id="btnYTD" onclick="toggleView('ytd')">YTD</button>
    <button class="toggle-btn" id="btnCustom" onclick="toggleView('custom')">Custom</button>
    <div class="filter-row" id="dateRow" style="display:none">
      <label>From</label><input type="date" id="startDate">
      <label>To</label><input type="date" id="endDate">
      <button class="analyze-btn" id="analyzeBtn" onclick="loadCustom()">Analyze</button>
    </div>
    <span class="period-label" id="periodLabel"></span>
    <span class="prior-label" id="priorLabel"></span>
  </div>
  <div id="staleBanner" class="stale-warning" style="display:none">
    &#9888; Bank transactions not yet uploaded for this period &mdash; expense-based metrics (COGS, Labor, Prime Cost, Net Margin, Marketing, OPEX, Rev/Labor Hr) are unavailable.
  </div>
  <div id="summaryBanner" class="banner" style="display:none"></div>
  <div id="scorecardSection"></div>
  <div id="financialSection"></div>
  <div id="operationalSection"></div>
  <div id="guestSection"></div>
  <div id="insightsSection"></div>
  <div id="trendSection"></div>
  <div id="legendSection"></div>
  <div id="loadingMsg" class="loading">Loading KPI data&hellip;</div>
</div>
<script>
(function(){
  const $=id=>document.getElementById(id);
  const fmt=n=>'$'+Number(n||0).toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
  const fmtD=n=>'$'+Number(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  const fmtPct=n=>Number(n||0).toFixed(1)+'%';
  const fmtNum=n=>Number(n||0).toLocaleString('en-US',{minimumFractionDigits:1,maximumFractionDigits:1});

  function fmtVal(v,f){
    if(f==='pct') return fmtPct(v);
    if(f==='dollar') return fmtD(v);
    return fmtNum(v);
  }

  let currentView='mtd', mtdData=null, ytdData=null, customData=null;

  function getDateRanges(){
    const now=new Date();
    const y=now.getFullYear(), m=now.getMonth(), d=now.getDate();
    const pad=n=>String(n).padStart(2,'0');
    const today=y+'-'+pad(m+1)+'-'+pad(d);
    return {
      mtd:{start_date:y+'-'+pad(m+1)+'-01', end_date:today},
      ytd:{start_date:y+'-01-01', end_date:today}
    };
  }

  async function loadBoth(){
    const r=getDateRanges();
    // Set default date picker values
    $('startDate').value=r.mtd.start_date;
    $('endDate').value=r.mtd.end_date;
    try{
      const [mResp,yResp]=await Promise.all([
        fetch('/api/kpi-benchmarks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(r.mtd)}),
        fetch('/api/kpi-benchmarks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(r.ytd)})
      ]);
      mtdData=await mResp.json();
      ytdData=await yResp.json();
      if(mtdData.error||ytdData.error){
        $('loadingMsg').textContent='Error: '+(mtdData.error||ytdData.error);
        return;
      }
      $('loadingMsg').style.display='none';
      render(currentView==='mtd'?mtdData:ytdData);
    }catch(err){
      $('loadingMsg').textContent='Error loading data: '+err.message;
    }
  }

  window.loadCustom=async function(){
    const sd=$('startDate').value, ed=$('endDate').value;
    if(!sd||!ed){alert('Select both dates');return;}
    $('analyzeBtn').disabled=true;
    $('analyzeBtn').textContent='Loading\u2026';
    try{
      const resp=await fetch('/api/kpi-benchmarks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:sd,end_date:ed})});
      customData=await resp.json();
      if(customData.error){alert('Error: '+customData.error);return;}
      currentView='custom';
      $('btnMTD').className='toggle-btn';
      $('btnYTD').className='toggle-btn';
      $('btnCustom').className='toggle-btn active';
      render(customData);
    }catch(err){alert('Error: '+err.message);}
    finally{$('analyzeBtn').disabled=false;$('analyzeBtn').textContent='Analyze';}
  };

  window.toggleView=function(v){
    currentView=v;
    $('btnMTD').className='toggle-btn'+(v==='mtd'?' active':'');
    $('btnYTD').className='toggle-btn'+(v==='ytd'?' active':'');
    $('btnCustom').className='toggle-btn'+(v==='custom'?' active':'');
    $('dateRow').style.display=v==='custom'?'flex':'none';
    if(v==='custom'){
      if(customData) render(customData);
      return;
    }
    render(v==='mtd'?mtdData:ytdData);
  };

  function statusClr(s){return s==='good'?'#22c55e':s==='watch'?'#f59e0b':'#ef4444';}
  function statusCls(s){return 'clr-'+s;}
  function dotCls(s){return 'dot-'+s;}

  function deltaHtml(delta,direction,format){
    if(delta===0) return '<span style="color:#6b7280">&mdash; 0</span>';
    const pos=delta>0;
    const isGood=(direction==='higher_is_better'&&pos)||(direction==='lower_is_better'&&!pos);
    const arrow=pos?'&#9650;':'&#9660;';
    const clr=isGood?'#22c55e':'#ef4444';
    const dv=format==='pct'?Math.abs(delta).toFixed(1)+'pp':format==='dollar'?'$'+Math.abs(delta).toFixed(0):Math.abs(delta).toFixed(1);
    return '<span style="color:'+clr+'">'+arrow+' '+dv+'</span>';
  }

  function benchText(b,benchmarks){
    const info=benchmarks[b.key];
    if(!info) return '';
    if(b.direction==='lower_is_better') return 'Target: \\u2264'+info.good_max+(b.format==='pct'?'%':'');
    return 'Target: \\u2265'+(b.format==='dollar'?'$':'')+info.good_min+(b.format==='pct'?'%':'');
  }

  function render(d){
    // Period labels
    $('periodLabel').textContent=currentView.toUpperCase()+': '+d.period.start+' to '+d.period.end;
    $('priorLabel').textContent='vs '+d.prior_period.start+' to '+d.prior_period.end;

    // Stale warning
    $('staleBanner').style.display=d.has_bank_data?'none':'block';

    renderSummary(d.summary);
    renderScorecard(d.scorecard,d.benchmarks);
    renderFinancial(d);
    renderOperational(d);
    renderGuest(d);
    renderInsights(d.insights||[]);
    renderTrends(d.trends,d.benchmarks);
    renderLegend(d.benchmarks);
  }

  function renderSummary(s){
    const b=$('summaryBanner');
    b.style.display='flex';
    b.innerHTML=
      '<span class="dot dot-good"></span><span class="stat">'+s.good+'<span>Green</span></span>'+
      '<span class="dot dot-watch"></span><span class="stat">'+s.watch+'<span>Watch</span></span>'+
      '<span class="dot dot-critical"></span><span class="stat">'+s.critical+'<span>Critical</span></span>'+
      '<span style="margin-left:auto;font-size:13px;color:#9ca3af">'+s.good+' of '+s.total+' metrics on target</span>';
  }

  function renderScorecard(sc,benchmarks){
    let h='<div class="section-title">KPI Scorecard</div><div class="grid-4">';
    sc.forEach(m=>{
      h+='<div class="card"><div class="card-label">'+m.label+
        ' <span class="dot '+dotCls(m.status)+'" title="'+m.status+'"></span></div>'+
        '<div class="card-value '+statusCls(m.status)+'">'+fmtVal(m.value,m.format)+'</div>'+
        '<div class="card-delta">'+deltaHtml(m.delta,m.direction,m.format)+
        ' <span style="color:#6b7280;font-size:11px">from '+fmtVal(m.prior,m.format)+'</span></div>'+
        '<div class="card-bench">'+benchText(m,benchmarks)+'</div></div>';
    });
    h+='</div>';
    $('scorecardSection').innerHTML=h;
  }

  function renderFinancial(d){
    const fin=['cogs_pct','labor_pct','prime_cost_pct','net_margin_pct','marketing_pct','opex_pct'];
    const sc=d.scorecard.filter(s=>fin.includes(s.key));
    let h='<div class="section-title">Financial Health</div>'+
      '<div style="display:flex;gap:16px;margin-bottom:16px">'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Adjusted Revenue</div><div class="card-value" style="color:#a5b4fc">'+fmt(d.adjusted_revenue)+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Net Profit</div><div class="card-value" style="color:'+(d.net_profit>=0?'#22c55e':'#ef4444')+'">'+fmt(d.net_profit)+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Operating Days</div><div class="card-value" style="color:#818cf8">'+d.operating_days+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Orders</div><div class="card-value" style="color:#818cf8">'+Number(d.order_count).toLocaleString()+'</div></div>'+
      '</div>';
    sc.forEach(m=>{
      const isHigher=m.direction==='higher_is_better';
      const maxVal=isHigher?30:Math.max(m.value*1.3,60);
      const pct=Math.min(Math.max(m.value/maxVal*100,2),100);
      h+='<div class="gauge-row">'+
        '<div class="gauge-label">'+m.label+'</div>'+
        '<div class="gauge-wrap"><div class="gauge-fill" style="width:'+pct+'%;background:'+statusClr(m.status)+'"></div></div>'+
        '<div class="gauge-val '+statusCls(m.status)+'">'+fmtPct(m.value)+'</div>'+
        '<div class="gauge-delta">'+deltaHtml(m.delta,m.direction,m.format)+'</div>'+
        '<div class="gauge-status '+dotCls(m.status)+'"></div>'+
        '</div>';
    });
    $('financialSection').innerHTML=h;
  }

  function renderOperational(d){
    const ops=['avg_check','orders_per_day','void_rate_pct','discount_rate_pct','rev_per_labor_hour'];
    const sc=d.scorecard.filter(s=>ops.includes(s.key));
    let h='<div class="section-title">Operational Efficiency</div><div class="grid-4">';
    sc.forEach(m=>{
      h+='<div class="card"><div class="card-label">'+m.label+
        ' <span class="dot '+dotCls(m.status)+'"></span></div>'+
        '<div class="card-value '+statusCls(m.status)+'">'+fmtVal(m.value,m.format)+'</div>'+
        '<div class="card-delta">'+deltaHtml(m.delta,m.direction,m.format)+
        ' <span style="color:#6b7280;font-size:11px">from '+fmtVal(m.prior,m.format)+'</span></div>'+
        '<div class="card-bench">'+benchText(m,d.benchmarks)+'</div></div>';
    });
    h+='</div>';
    $('operationalSection').innerHTML=h;
  }

  function renderGuest(d){
    const gKeys=['repeat_guest_pct','repeat_rev_pct','at_risk_pct'];
    const sc=d.scorecard.filter(s=>gKeys.includes(s.key));
    const g=d.guest;
    let h='<div class="section-title">Guest Intelligence</div>'+
      '<div style="display:flex;gap:16px;margin-bottom:16px">'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Total Guests</div><div class="card-value" style="color:#a5b4fc">'+Number(g.total_guests).toLocaleString()+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Repeat Guests</div><div class="card-value" style="color:#22c55e">'+Number(g.repeat_guests).toLocaleString()+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">At-Risk</div><div class="card-value" style="color:#f59e0b">'+g.at_risk_count+'</div></div>'+
      '<div class="card" style="flex:1;text-align:center"><div class="card-label">Repeat Revenue</div><div class="card-value" style="color:#22c55e">'+fmt(g.repeat_revenue)+'</div></div>'+
      '</div><div class="grid-4">';
    sc.forEach(m=>{
      h+='<div class="card"><div class="card-label">'+m.label+
        ' <span class="dot '+dotCls(m.status)+'"></span></div>'+
        '<div class="card-value '+statusCls(m.status)+'">'+fmtVal(m.value,m.format)+'</div>'+
        '<div class="card-delta">'+deltaHtml(m.delta,m.direction,m.format)+
        ' <span style="color:#6b7280;font-size:11px">from '+fmtVal(m.prior,m.format)+'</span></div>'+
        '<div class="card-bench">'+benchText(m,d.benchmarks)+'</div></div>';
    });
    h+='</div>';
    $('guestSection').innerHTML=h;
  }

  function renderInsights(insights){
    if(!insights.length){$('insightsSection').innerHTML='';return;}
    let h='<div class="section-title">Key Insights &amp; Recommendations</div>';
    // Show watch/critical first, then good
    const sorted=[...insights].sort((a,b)=>{
      const order={critical:0,watch:1,good:2};
      return (order[a.status]||2)-(order[b.status]||2);
    });
    sorted.forEach(i=>{
      h+='<div class="insight-card">'+
        '<div class="insight-dot '+dotCls(i.status)+'"></div>'+
        '<div><div class="insight-label">'+i.label+'</div>'+
        '<div class="insight-text">'+i.insight+'</div></div></div>';
    });
    $('insightsSection').innerHTML=h;
  }

  function renderTrends(trends,benchmarks){
    const keys=['adjusted_revenue','cogs_pct','labor_pct','prime_cost_pct','net_margin_pct','avg_check'];
    const labels={'adjusted_revenue':'Revenue','cogs_pct':'COGS %','labor_pct':'Labor %','prime_cost_pct':'Prime Cost %','net_margin_pct':'Net Margin %','avg_check':'Avg Check'};
    const formats={'adjusted_revenue':'dollar','cogs_pct':'pct','labor_pct':'pct','prime_cost_pct':'pct','net_margin_pct':'pct','avg_check':'dollar'};
    let h='<div class="section-title">6-Month Trends</div><div class="sparkline-grid">';
    keys.forEach(key=>{
      const vals=trends[key]||[];
      const cur=vals.length?vals[vals.length-1]:0;
      const maxV=Math.max(...vals,1);
      const bi=benchmarks[key];
      const benchVal=bi?(bi.direction==='lower_is_better'?bi.good_max:bi.good_min):null;
      const benchPct=benchVal!==null?Math.min(benchVal/maxV*100,100):null;
      h+='<div class="spark-card"><div class="spark-label">'+labels[key]+'</div>'+
        '<div class="spark-val">'+fmtVal(cur,formats[key])+'</div>'+
        '<div class="sparkline">';
      vals.forEach((v,i)=>{
        const pct=Math.max(v/maxV*100,3);
        const clr=i===vals.length-1?'#818cf8':'#4f46e5';
        h+='<div class="bar" style="height:'+pct+'%;background:'+clr+'" title="'+trends.months[i]+': '+fmtVal(v,formats[key])+'"></div>';
      });
      if(benchPct!==null) h+='<div class="bench-line" style="bottom:'+benchPct+'%" title="Benchmark"></div>';
      h+='</div></div>';
    });
    h+='</div>';
    $('trendSection').innerHTML=h;
  }

  function renderLegend(benchmarks){
    let h='<details class="legend"><summary>Benchmark Definitions &amp; Sources</summary><table><thead><tr>'+
      '<th>Metric</th><th>Good</th><th>Watch</th><th>Critical</th><th>Source</th></tr></thead><tbody>';
    Object.entries(benchmarks).forEach(([k,b])=>{
      const d=b.direction;
      if(d==='lower_is_better'){
        h+='<tr><td>'+b.label+'</td><td class="clr-good">\\u2264 '+b.good_max+'</td>'+
          '<td class="clr-watch">\\u2264 '+b.watch_max+'</td>'+
          '<td class="clr-critical">&gt; '+b.watch_max+'</td>'+
          '<td class="src">'+b.source+'</td></tr>';
      }else{
        h+='<tr><td>'+b.label+'</td><td class="clr-good">\\u2265 '+b.good_min+'</td>'+
          '<td class="clr-watch">\\u2265 '+b.watch_min+'</td>'+
          '<td class="clr-critical">&lt; '+b.watch_min+'</td>'+
          '<td class="src">'+b.source+'</td></tr>';
      }
    });
    h+='</tbody></table></details>';
    $('legendSection').innerHTML=h;
  }

  loadBoth();
})();
</script>
</body>
</html>'''



def _budget_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Budget Tracker</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e2e8f0;min-height:100vh}
.header{background:linear-gradient(135deg,#059669,#10b981,#34d399);padding:24px 32px;text-align:center}
.header h1{font-size:24px;font-weight:700;color:#fff}
.header p{color:rgba(255,255,255,.85);font-size:13px;margin-top:4px}
.nav{display:flex;gap:4px;padding:8px 16px;background:#1a1a2e;flex-wrap:wrap;justify-content:center}
.nav a{color:#94a3b8;text-decoration:none;padding:6px 14px;border-radius:6px;font-size:13px;transition:.2s}
.nav a:hover{background:#334155;color:#e2e8f0}
.nav a.active{background:#059669;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:20px}
.filter-bar{display:flex;gap:12px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
.filter-bar label{color:#94a3b8;font-size:13px}
.filter-bar input[type="month"]{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 12px;border-radius:6px;font-size:14px}
.filter-bar button{background:#059669;color:#fff;border:none;padding:8px 20px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600}
.filter-bar button:hover{background:#047857}
.banner{border-radius:12px;padding:24px 32px;margin-bottom:20px;text-align:center}
.banner.good{background:linear-gradient(135deg,#059669,#10b981)}
.banner.watch{background:linear-gradient(135deg,#d97706,#f59e0b)}
.banner.critical{background:linear-gradient(135deg,#dc2626,#ef4444)}
.banner h2{font-size:28px;color:#fff;font-weight:700}
.banner p{color:rgba(255,255,255,.9);font-size:15px;margin-top:6px}
.grid4{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:20px}
@media(max-width:1024px){.grid4{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.grid4{grid-template-columns:1fr}}
.budget-card{background:#1e293b;border-radius:12px;padding:20px;border:1px solid #334155}
.budget-card .card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.budget-card .card-title{font-size:14px;font-weight:600;color:#e2e8f0}
.budget-card .card-status{font-size:11px;font-weight:700;padding:3px 8px;border-radius:4px;text-transform:uppercase}
.status-under_budget{background:#065f46;color:#34d399}
.status-on_track{background:#92400e;color:#fbbf24}
.status-watch{background:#7c2d12;color:#fb923c}
.status-over_budget{background:#7f1d1d;color:#fca5a5}
.budget-card .amounts{display:flex;justify-content:space-between;margin-bottom:8px}
.budget-card .amt-actual{font-size:22px;font-weight:700;color:#e2e8f0}
.budget-card .amt-target{font-size:13px;color:#94a3b8}
.gauge-bar{height:8px;background:#334155;border-radius:4px;overflow:hidden;position:relative;margin-bottom:6px}
.gauge-fill{height:100%;border-radius:4px;transition:width .5s ease}
.pct-row{display:flex;justify-content:space-between;font-size:12px;color:#94a3b8;margin-bottom:10px}
.vendor-list{font-size:11px;color:#64748b}
.vendor-list div{padding:2px 0;display:flex;justify-content:space-between}
.vendor-list .v-name{color:#94a3b8}
.vendor-list .v-amt{color:#cbd5e1}
.section{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #334155}
.section h3{font-size:16px;font-weight:600;color:#e2e8f0;margin-bottom:16px}
.waterfall{display:flex;align-items:flex-end;gap:8px;height:260px;padding-top:20px;justify-content:center}
.wf-bar{display:flex;flex-direction:column;align-items:center;min-width:80px}
.wf-bar .bar{width:60px;border-radius:4px 4px 0 0;transition:height .5s ease;position:relative}
.wf-bar .bar-label{font-size:11px;color:#94a3b8;margin-top:6px;text-align:center}
.wf-bar .bar-value{font-size:12px;font-weight:600;color:#e2e8f0;margin-bottom:4px}
.trend-chart{position:relative;height:300px;margin:16px 0}
.trend-chart canvas{width:100%!important;height:100%!important}
.path-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-bottom:20px}
.path-card{background:#1e293b;border-radius:12px;padding:20px;border-left:4px solid #059669}
.path-card.p1{border-left-color:#ef4444}
.path-card.p2{border-left-color:#f59e0b}
.path-card.p3{border-left-color:#3b82f6}
.path-card.p4{border-left-color:#8b5cf6}
.path-card .p-label{font-size:13px;color:#94a3b8;margin-bottom:4px}
.path-card .p-pct{font-size:20px;font-weight:700;color:#e2e8f0}
.path-card .p-savings{font-size:14px;color:#34d399;font-weight:600;margin:6px 0}
.path-card .p-insight{font-size:12px;color:#94a3b8;line-height:1.4}
.vendor-table{width:100%;border-collapse:collapse}
.vendor-table th{text-align:left;padding:10px 12px;border-bottom:2px solid #334155;color:#94a3b8;font-size:12px;text-transform:uppercase;cursor:pointer}
.vendor-table th:hover{color:#e2e8f0}
.vendor-table td{padding:8px 12px;border-bottom:1px solid #1e293b;font-size:13px;color:#e2e8f0}
.vendor-table tr:hover td{background:#334155}
.vendor-table .amt{text-align:right;font-variant-numeric:tabular-nums}
.insight-card{padding:14px 18px;border-radius:8px;margin-bottom:10px;border-left:4px solid #334155}
.insight-card.sev-critical{background:#7f1d1d22;border-left-color:#ef4444}
.insight-card.sev-warning{background:#7c2d1222;border-left-color:#f59e0b}
.insight-card.sev-info{background:#1e3a5f22;border-left-color:#3b82f6}
.insight-card.sev-good{background:#06563022;border-left-color:#10b981}
.insight-card .sev-badge{font-size:10px;font-weight:700;text-transform:uppercase;margin-bottom:4px}
.insight-card .sev-badge.critical{color:#fca5a5}
.insight-card .sev-badge.warning{color:#fbbf24}
.insight-card .sev-badge.info{color:#93c5fd}
.insight-card .sev-badge.good{color:#6ee7b7}
.insight-card .insight-text{font-size:13px;color:#cbd5e1;line-height:1.5}
.loading{text-align:center;padding:60px;color:#64748b}
.total-savings-banner{background:linear-gradient(135deg,#059669,#047857);border-radius:12px;padding:20px 28px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}
.total-savings-banner .ts-label{font-size:14px;color:rgba(255,255,255,.85)}
.total-savings-banner .ts-value{font-size:28px;font-weight:700;color:#fff}
.section-group{margin-bottom:20px}
.section-group-header{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:16px 20px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;transition:background .2s}
.section-group-header:hover{background:#263548}
.section-group-header .sg-title{font-size:15px;font-weight:600;color:#e2e8f0}
.section-group-header .sg-right{display:flex;gap:10px;align-items:center}
.section-group-header .sg-badge{font-size:11px;font-weight:700;padding:3px 8px;border-radius:4px}
.section-group-header .sg-badge.off{background:#7f1d1d;color:#fca5a5}
.section-group-header .sg-badge.ok{background:#065f46;color:#34d399}
.section-group-header .sg-arrow{font-size:18px;color:#64748b;transition:transform .3s}
.section-group-header.open .sg-arrow{transform:rotate(180deg)}
.section-group-body{display:none;padding:16px 0 0}
.section-group-body.open{display:block}
.sub-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
@media(max-width:1024px){.sub-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.sub-grid{grid-template-columns:1fr}}
.sub-card{background:#162032;border-radius:10px;padding:16px;border:1px solid #334155}
.sub-card.informational{border:1px dashed #475569;opacity:.7}
.sub-card .sc-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.sub-card .sc-title{font-size:13px;font-weight:600;color:#cbd5e1}
.sub-card .sc-status{font-size:10px;font-weight:700;padding:2px 6px;border-radius:3px;text-transform:uppercase}
.sub-card .sc-amounts{display:flex;justify-content:space-between;margin-bottom:6px}
.sub-card .sc-actual{font-size:18px;font-weight:700;color:#e2e8f0}
.sub-card .sc-target{font-size:11px;color:#94a3b8}
.sub-card .sc-gauge{height:6px;background:#334155;border-radius:3px;overflow:hidden;margin-bottom:4px}
.sub-card .sc-gauge-fill{height:100%;border-radius:3px}
.sub-card .sc-pct{display:flex;justify-content:space-between;font-size:11px;color:#94a3b8;margin-bottom:8px}
.sub-card .sc-vendors{font-size:10px;color:#64748b}
.sub-card .sc-vendors div{padding:1px 0;display:flex;justify-content:space-between}
.sub-card .sc-vendors .sv-name{color:#94a3b8}
.sub-card .sc-vendors .sv-amt{color:#cbd5e1}
.unbudgeted-card{background:#1e293b;border-radius:10px;padding:16px;border-left:3px solid #64748b;margin-bottom:12px}
.unbudgeted-card .ub-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.unbudgeted-card .ub-title{font-size:14px;font-weight:600;color:#e2e8f0}
.unbudgeted-card .ub-amt{font-size:18px;font-weight:700;color:#e2e8f0}
.unbudgeted-card .ub-note{font-size:11px;color:#64748b;font-style:italic;margin-bottom:8px}
.unbudgeted-card .ub-vendors{font-size:10px;color:#64748b}
.unbudgeted-card .ub-vendors div{padding:1px 0;display:flex;justify-content:space-between}
.budget-card{cursor:pointer;transition:background .2s}
.budget-card:hover{background:#263548}
.budget-card .alert-badge{font-size:10px;font-weight:700;padding:2px 6px;border-radius:10px;background:#7f1d1d;color:#fca5a5;margin-left:6px}
.sub-card .sc-txns{font-size:10px;color:#64748b;margin-top:4px}
.sub-card .sc-txns .txn-row{padding:3px 0;display:grid;grid-template-columns:58px 1fr auto;gap:6px;border-bottom:1px solid #1e293b}
.sub-card .sc-txns .txn-date{color:#64748b;font-size:10px}
.sub-card .sc-txns .txn-desc{color:#94a3b8;font-size:10px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sub-card .sc-txns .txn-amt{color:#cbd5e1;font-size:10px;text-align:right;font-variant-numeric:tabular-nums}
.view-all-btn{display:inline-block;margin-top:6px;font-size:10px;color:#34d399;cursor:pointer;text-decoration:underline;background:none;border:none;padding:0}
.view-all-btn:hover{color:#6ee7b7}
.drilldown-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.7);z-index:1000;display:flex;align-items:center;justify-content:center}
.drilldown-panel{background:#1e293b;border-radius:12px;padding:24px;width:90%;max-width:800px;max-height:80vh;overflow-y:auto;border:1px solid #334155}
.drilldown-panel h3{font-size:16px;color:#e2e8f0;margin-bottom:4px}
.drilldown-panel .dd-meta{font-size:12px;color:#94a3b8;margin-bottom:16px}
.drilldown-panel .dd-close{float:right;background:none;border:none;color:#94a3b8;font-size:20px;cursor:pointer;line-height:1}
.drilldown-panel .dd-close:hover{color:#e2e8f0}
.drilldown-table{width:100%;border-collapse:collapse}
.drilldown-table th{text-align:left;padding:8px 10px;border-bottom:2px solid #334155;color:#94a3b8;font-size:11px;text-transform:uppercase}
.drilldown-table td{padding:6px 10px;border-bottom:1px solid #162032;font-size:12px;color:#e2e8f0}
.drilldown-table tr:hover td{background:#263548}
.drilldown-table .amt{text-align:right;font-variant-numeric:tabular-nums}
.revenue-card{background:#1e293b;border-radius:12px;padding:24px;border-left:4px solid #10b981;margin-bottom:20px}
.rev-topline{display:flex;gap:40px;align-items:baseline;margin-bottom:20px;flex-wrap:wrap}
.rev-topline .rev-block{display:flex;flex-direction:column}
.rev-topline .rev-label{font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}
.rev-topline .rev-gross{font-size:32px;font-weight:700;color:#e2e8f0}
.rev-topline .rev-net{font-size:28px;font-weight:700;color:#34d399}
.rev-topline .rev-adj{font-size:16px;color:#94a3b8;margin-top:2px}
.rev-mix{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
@media(max-width:768px){.rev-mix{grid-template-columns:repeat(2,1fr)}}
.rev-mix-item{background:#162032;border-radius:8px;padding:12px}
.rev-mix-item .rm-label{font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.rev-mix-item .rm-amount{font-size:20px;font-weight:700;color:#e2e8f0;margin-bottom:6px}
.rev-mix-item .rm-bar{height:6px;background:#334155;border-radius:3px;overflow:hidden;margin-bottom:4px}
.rev-mix-item .rm-bar-fill{height:100%;border-radius:3px}
.rev-mix-item .rm-pct{font-size:11px;color:#64748b;text-align:right}
.rev-grat{display:grid;grid-template-columns:1fr 1fr 2fr;gap:14px;padding-top:16px;border-top:1px solid #334155}
@media(max-width:768px){.rev-grat{grid-template-columns:1fr}}
.rev-grat-block{background:#162032;border-radius:8px;padding:12px}
.rev-grat-block .rg-label{font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.rev-grat-block .rg-amount{font-size:18px;font-weight:700;color:#e2e8f0}
.rev-grat-block .rg-sub{font-size:12px;color:#94a3b8;margin-top:6px;line-height:1.6}
.rev-grat-block .rg-lov3{color:#34d399;font-weight:600}
.rev-grat-block .rg-staff{color:#60a5fa;font-weight:600}
.rev-section-title{font-size:13px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;font-weight:600}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Budget Tracker</h1>
  <p>Monthly spending performance vs 15% profit margin target</p>
  <p style="max-width:720px;margin:8px auto 0;font-size:12px;color:rgba(255,255,255,.7);line-height:1.5">Are you spending within your means? This dashboard compares actual monthly expenses to budget targets across COGS, Labor, Marketing, and OPEX. Use it to spot cost overruns before they erode margins, identify which vendors are driving overspend, and prioritize the categories with the biggest savings opportunity on the path to 15% profit.</p>
</div>
<div class="nav">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L</a>
  <a href="/analysis">Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget" class="active">Budget</a>
</div>
<div class="container">
  <div class="filter-bar">
    <label>Month:</label>
    <input type="month" id="monthPicker">
    <button id="btnAnalyze">Analyze</button>
  </div>
  <div id="content"><div class="loading">Loading budget data&hellip;</div></div>
</div>
<script>
(function(){
  const $ = id => document.getElementById(id);
  const fmt = n => n == null ? '--' : '$' + Math.abs(n).toLocaleString('en-US', {maximumFractionDigits:0});
  const fmtK = n => n == null ? '--' : (Math.abs(n) >= 1000 ? '$' + (Math.abs(n)/1000).toFixed(1) + 'K' : '$' + Math.abs(n).toFixed(0));
  const pct = n => n == null ? '--' : n.toFixed(1) + '%';
  const sign = n => n > 0 ? '+' : '';

  // Set default month
  const now = new Date();
  $('monthPicker').value = now.getFullYear() + '-' + String(now.getMonth()+1).padStart(2,'0');

  function loadBudget(){
    const month = $('monthPicker').value;
    if(!month) return;
    $('content').innerHTML = '<div class="loading">Loading budget data&hellip;</div>';
    fetch('/api/budget', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({month})
    })
    .then(r => r.json())
    .then(data => {
      if(data.error){ $('content').innerHTML = '<div class="loading" style="color:#fca5a5">Error: '+data.error+'</div>'; return; }
      render(data);
    })
    .catch(e => { $('content').innerHTML = '<div class="loading" style="color:#fca5a5">'+e+'</div>'; });
  }

  window.toggleSection = function(parentKey){
    const hdr = document.querySelector(`[data-sg="${parentKey}"]`);
    const body = document.querySelector(`[data-sg-body="${parentKey}"]`);
    if(!hdr || !body) return;
    hdr.classList.toggle('open');
    body.classList.toggle('open');
  };

  window.loadDrilldown = function(subKey, label){
    const month = $('monthPicker').value;
    if(!month) return;
    const overlay = document.createElement('div');
    overlay.className = 'drilldown-overlay';
    overlay.id = 'drilldown-overlay';
    overlay.innerHTML = `<div class="drilldown-panel">
      <button class="dd-close" onclick="closeDrilldown()">&times;</button>
      <h3>${label}</h3>
      <div class="dd-meta">Loading transactions&hellip;</div>
    </div>`;
    overlay.addEventListener('click', function(e){ if(e.target === overlay) closeDrilldown(); });
    document.body.appendChild(overlay);
    fetch('/api/budget-drilldown', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({month, subcategory:subKey})
    })
    .then(r => r.json())
    .then(data => {
      if(data.error){
        document.querySelector('#drilldown-overlay .dd-meta').textContent = 'Error: ' + data.error;
        return;
      }
      const panel = document.querySelector('#drilldown-overlay .drilldown-panel');
      const ml = new Date(month + '-15').toLocaleDateString('en-US',{month:'long',year:'numeric'});
      let h = `<button class="dd-close" onclick="closeDrilldown()">&times;</button>`;
      h += `<h3>${data.label}</h3>`;
      h += `<div class="dd-meta">${ml} &mdash; ${data.count} transactions &mdash; Total: ${fmt(data.total)}</div>`;
      h += `<table class="drilldown-table"><thead><tr>
        <th>Date</th><th>Vendor</th><th>Description</th><th class="amt">Amount</th>
      </tr></thead><tbody>`;
      data.transactions.forEach(t => {
        h += `<tr>
          <td>${t.date}</td>
          <td>${t.vendor || '&mdash;'}</td>
          <td style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(t.description||'').replace(/"/g,'&quot;')}">${t.description || '&mdash;'}</td>
          <td class="amt">${fmt(t.amount)}</td>
        </tr>`;
      });
      h += '</tbody></table>';
      panel.innerHTML = h;
    })
    .catch(e => {
      document.querySelector('#drilldown-overlay .dd-meta').textContent = 'Error: ' + e;
    });
  };

  window.closeDrilldown = function(){
    const overlay = document.getElementById('drilldown-overlay');
    if(overlay) overlay.remove();
  };

  function render(d){
    const t = d.totals;
    const monthLabel = new Date(d.month + '-15').toLocaleDateString('en-US',{month:'long',year:'numeric'});
    let html = '';
    const catColors = {under_budget:'#10b981',on_track:'#f59e0b',watch:'#fb923c',over_budget:'#ef4444',informational:'#64748b'};
    const statusLabels = {over_budget:'OVER',watch:'WATCH',on_track:'ON TRACK',under_budget:'ON TARGET',informational:'INFO',unknown:'—'};

    // Count off-target subcategories per parent
    const subs = d.subcategories || {};
    const offTargetCounts = {};
    Object.values(subs).forEach(s => {
      if(!offTargetCounts[s.parent]) offTargetCounts[s.parent] = 0;
      if(s.status === 'over_budget' || s.status === 'watch') offTargetCounts[s.parent]++;
    });

    // Banner
    const bannerClass = t.margin_status;
    if(t.margin_pct < 0){
      html += `<div class="banner ${bannerClass}"><h2>${monthLabel}: ${pct(t.margin_pct)} Margin</h2><p>Operating at a loss &mdash; ${fmt(Math.abs(t.net_profit))}/mo gap to reach ${pct(t.target_margin)} target</p></div>`;
    } else if(t.margin_pct < t.target_margin){
      const gap = (d.revenue.adjusted_revenue * t.target_margin / 100 - t.net_profit);
      html += `<div class="banner ${bannerClass}"><h2>${monthLabel}: ${pct(t.margin_pct)} Margin</h2><p>${fmt(gap)}/mo short of ${pct(t.target_margin)} target</p></div>`;
    } else {
      html += `<div class="banner ${bannerClass}"><h2>${monthLabel}: ${pct(t.margin_pct)} Margin</h2><p>Above the ${pct(t.target_margin)} target &mdash; keep it up!</p></div>`;
    }

    // Revenue Position Card
    const rv = d.revenue || {};
    if(rv.gross_revenue != null){
      const mixItems = [
        {label:'Liquor', amt:rv.liquor, color:'#8b5cf6'},
        {label:'Food', amt:rv.food, color:'#f59e0b'},
        {label:'Hookah', amt:rv.hookah, color:'#ec4899'},
        {label:'Other', amt:rv.other, color:'#6b7280'},
      ].sort((a,b) => b.amt - a.amt);
      const mixMax = Math.max(...mixItems.map(m => m.amt), 1);
      const totalRev = (rv.net_sales || 0) + (rv.hookah || 0);
      html += `<div class="revenue-card">`;
      html += `<div class="rev-topline">`;
      html += `<div class="rev-block"><span class="rev-label">Gross Revenue</span><span class="rev-gross">${fmt(rv.gross_revenue)}</span></div>`;
      html += `<div class="rev-block"><span class="rev-label">Net Sales (POS)</span><span class="rev-net">${fmt(rv.net_sales)}</span></div>`;
      html += `<div class="rev-block"><span class="rev-label">Hookah (Bank)</span><span class="rev-net">${fmt(rv.hookah)}</span></div>`;
      html += `<div class="rev-block"><span class="rev-label">Orders</span><span class="rev-adj">${(rv.order_count||0).toLocaleString()}</span></div>`;
      html += `</div>`;
      html += `<div class="rev-section-title">Revenue Mix</div>`;
      html += `<div class="rev-mix">`;
      mixItems.forEach(m => {
        const pctOfNet = totalRev > 0 ? (m.amt / totalRev * 100) : 0;
        const barW = m.amt / mixMax * 100;
        html += `<div class="rev-mix-item">
          <div class="rm-label">${m.label}</div>
          <div class="rm-amount">${fmt(m.amt)}</div>
          <div class="rm-bar"><div class="rm-bar-fill" style="width:${barW.toFixed(1)}%;background:${m.color}"></div></div>
          <div class="rm-pct">${pctOfNet.toFixed(1)}% of revenue</div>
        </div>`;
      });
      html += `</div>`;
      html += `<div class="rev-section-title">Tips &amp; Gratuity Flow</div>`;
      html += `<div class="rev-grat">`;
      html += `<div class="rev-grat-block"><div class="rg-label">Tips</div><div class="rg-amount">${fmt(rv.total_tips)}</div><div class="rg-sub">100% &rarr; <span class="rg-staff">Staff</span></div></div>`;
      html += `<div class="rev-grat-block"><div class="rg-label">Gratuity</div><div class="rg-amount">${fmt(rv.total_gratuity)}</div><div class="rg-sub">65% &rarr; Staff &bull; 35% &rarr; LOV3</div></div>`;
      const staffTotal = (rv.pass_through || 0);
      const lov3Total = (rv.gratuity_retained || 0);
      html += `<div class="rev-grat-block"><div class="rg-label">Split</div><div class="rg-amount">${fmt(staffTotal + lov3Total)}</div><div class="rg-sub"><span class="rg-lov3">LOV3 (35% grat): ${fmt(lov3Total)}</span><br><span class="rg-staff">Staff (65% grat + tips): ${fmt(staffTotal)}</span></div></div>`;
      html += `</div>`;
      html += `</div>`;
    }

    // Budget Cards — clicking scrolls to subcategory section
    html += '<div class="grid4">';
    const cats = ['cogs','labor','marketing','opex','ga','facility'];
    cats.forEach(key => {
      const b = d.budget[key];
      if(!b) return;
      const fillPct = Math.min(b.actual_pct / b.max_pct * 100, 100);
      const fillColor = catColors[b.status] || '#10b981';
      const varSign = b.variance > 0 ? '+' : '';
      const offCount = offTargetCounts[key] || 0;
      const alertHtml = offCount > 0 ? `<span class="alert-badge">${offCount} off-target</span>` : '';
      html += `<div class="budget-card" onclick="toggleSection('${key}')">
        <div class="card-header">
          <span class="card-title">${b.label}${alertHtml}</span>
          <span class="card-status status-${b.status}">${b.status.replace('_',' ')}</span>
        </div>
        <div class="amounts">
          <span class="amt-actual">${fmt(b.actual)}</span>
          <span class="amt-target">Target: ${fmt(b.target_amount)}</span>
        </div>
        <div class="gauge-bar"><div class="gauge-fill" style="width:${fillPct}%;background:${fillColor}"></div></div>
        <div class="pct-row">
          <span>${pct(b.actual_pct)} actual</span>
          <span>${pct(b.target_pct)} target</span>
          <span style="color:${b.variance_pct > 0 ? '#fca5a5' : '#6ee7b7'}">${varSign}${fmt(b.variance)}</span>
        </div>
        <div class="vendor-list">`;
      (b.top_vendors||[]).forEach(v => {
        html += `<div><span class="v-name">${v.vendor}</span><span class="v-amt">${fmtK(v.amount)}</span></div>`;
      });
      html += '</div></div>';
    });
    html += '</div>';

    // Expandable Subcategory Sections
    cats.forEach(parentKey => {
      const parentLabel = d.budget[parentKey] ? d.budget[parentKey].label : parentKey;
      const parentSubs = Object.entries(subs).filter(([k,s]) => s.parent === parentKey);
      if(parentSubs.length === 0) return;

      // Sort by actual amount desc
      parentSubs.sort((a,b) => b[1].actual - a[1].actual);

      const offCount = offTargetCounts[parentKey] || 0;
      const badgeClass = offCount > 0 ? 'off' : 'ok';
      const badgeText = offCount > 0 ? `${offCount} off-target` : 'all on target';

      html += `<div class="section-group">`;
      html += `<div class="section-group-header" data-sg="${parentKey}" onclick="toggleSection('${parentKey}')">
        <span class="sg-title">${parentLabel} — Subcategory Breakdown</span>
        <span class="sg-right">
          <span class="sg-badge ${badgeClass}">${badgeText}</span>
          <span class="sg-arrow">&#9660;</span>
        </span>
      </div>`;
      html += `<div class="section-group-body" data-sg-body="${parentKey}">`;
      html += '<div class="sub-grid">';

      parentSubs.forEach(([subKey, s]) => {
        const isInfo = s.informational;
        const cardClass = isInfo ? 'sub-card informational' : 'sub-card';
        const sc = catColors[s.status] || '#64748b';
        const sl = statusLabels[s.status] || '—';
        const maxPct = s.target_pct > 0 ? s.target_pct * 1.5 : (s.actual_pct > 0 ? s.actual_pct : 1);
        const gaugePct = Math.min(s.actual_pct / maxPct * 100, 100);
        const varSign = s.variance > 0 ? '+' : '';

        html += `<div class="${cardClass}">
          <div class="sc-header">
            <span class="sc-title">${s.label}</span>
            <span class="sc-status" style="background:${sc}22;color:${sc}">${sl}</span>
          </div>
          <div class="sc-amounts">
            <span class="sc-actual">${fmt(s.actual)}</span>
            <span class="sc-target">${isInfo ? 'Pass-through' : 'Target: ' + fmt(s.target_amount)}</span>
          </div>`;
        if(!isInfo){
          html += `<div class="sc-gauge"><div class="sc-gauge-fill" style="width:${gaugePct}%;background:${sc}"></div></div>
          <div class="sc-pct">
            <span>${s.actual_pct.toFixed(1)}% actual</span>
            <span>${s.target_pct.toFixed(1)}% target</span>
            <span style="color:${s.variance > 0 ? '#fca5a5' : '#6ee7b7'}">${varSign}${fmt(s.variance)}</span>
          </div>`;
        }
        html += '<div class="sc-txns">';
        const txns = (s.top_transactions || []);
        if(txns.length > 0){
          txns.forEach(t => {
            const dt = t.date ? t.date.substring(5) : '';
            const desc = (t.vendor || t.description || '').substring(0, 28);
            html += `<div class="txn-row">
              <span class="txn-date">${dt}</span>
              <span class="txn-desc" title="${(t.description||'').replace(/"/g,'&quot;')}">${desc}</span>
              <span class="txn-amt">${fmtK(t.amount)}</span>
            </div>`;
          });
        } else {
          html += '<div style="color:#475569;font-style:italic;padding:4px 0">No transactions</div>';
        }
        if((s.transaction_count || 0) > 5){
          html += `<button class="view-all-btn" onclick="event.stopPropagation();loadDrilldown('${subKey}','${s.label.replace(/'/g,"\\\\'")}')">View All (${s.transaction_count})</button>`;
        } else if(txns.length > 0){
          html += `<div style="font-size:9px;color:#475569;margin-top:4px">${s.transaction_count || txns.length} total</div>`;
        }
        html += '</div></div>';
      });

      html += '</div></div></div>';
    });

    // Waterfall
    const rev = d.revenue.adjusted_revenue;
    const cogs = d.budget.cogs ? d.budget.cogs.actual : 0;
    const labor = d.budget.labor ? d.budget.labor.actual : 0;
    const mktg = d.budget.marketing ? d.budget.marketing.actual : 0;
    const opex = d.budget.opex ? d.budget.opex.actual : 0;
    const ga = d.budget.ga ? d.budget.ga.actual : 0;
    const fac = d.budget.facility ? d.budget.facility.actual : 0;
    const profit = t.net_profit;
    const maxVal = Math.max(rev, cogs + labor + mktg + opex + ga + fac, Math.abs(profit)) || 1;
    const barH = v => Math.max(Math.abs(v) / maxVal * 200, 4);
    html += `<div class="section"><h3>P&L Waterfall</h3><div class="waterfall">`;
    const wfBars = [
      {label:'Revenue', value:rev, color:'#10b981'},
      {label:'COGS', value:-cogs, color:'#ef4444'},
      {label:'Labor', value:-labor, color:'#f59e0b'},
      {label:'Marketing', value:-mktg, color:'#8b5cf6'},
      {label:'OPEX', value:-opex, color:'#fb923c'},
      {label:'G&A', value:-ga, color:'#64748b'},
      {label:'Facility', value:-fac, color:'#78716c'},
      {label:'Profit', value:profit, color:profit >= 0 ? '#059669' : '#dc2626'},
    ];
    wfBars.forEach(b => {
      html += `<div class="wf-bar">
        <div class="bar-value">${fmt(b.value)}</div>
        <div class="bar" style="height:${barH(b.value)}px;background:${b.color}"></div>
        <div class="bar-label">${b.label}</div>
      </div>`;
    });
    html += '</div></div>';

    // 12-Month Trend (simple bars)
    const hist = d.monthly_history;
    if(hist && hist.months && hist.months.length > 0){
      html += `<div class="section"><h3>12-Month Trend</h3>`;
      html += `<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12px">`;
      html += '<tr><th style="padding:6px;color:#94a3b8;text-align:left">Month</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">Revenue</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">COGS%</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">Labor%</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">Mktg%</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">OPEX%</th>';
      html += '<th style="padding:6px;color:#94a3b8;text-align:right">Margin%</th></tr>';
      hist.months.forEach((m, i) => {
        const marginColor = hist.margin_pct[i] >= 15 ? '#10b981' : hist.margin_pct[i] >= 0 ? '#fbbf24' : '#ef4444';
        const mLabel = new Date(m + '-15').toLocaleDateString('en-US',{month:'short',year:'2-digit'});
        html += `<tr style="border-bottom:1px solid #334155">
          <td style="padding:6px;color:#cbd5e1">${mLabel}</td>
          <td style="padding:6px;text-align:right;color:#e2e8f0">${fmtK(hist.revenue[i])}</td>
          <td style="padding:6px;text-align:right;color:${hist.cogs_pct[i] > 30 ? '#fca5a5' : '#e2e8f0'}">${pct(hist.cogs_pct[i])}</td>
          <td style="padding:6px;text-align:right;color:${hist.labor_pct[i] > 33 ? '#fca5a5' : '#e2e8f0'}">${pct(hist.labor_pct[i])}</td>
          <td style="padding:6px;text-align:right;color:${hist.marketing_pct[i] > 8 ? '#fca5a5' : '#e2e8f0'}">${pct(hist.marketing_pct[i])}</td>
          <td style="padding:6px;text-align:right;color:${hist.opex_pct[i] > 18 ? '#fca5a5' : '#e2e8f0'}">${pct(hist.opex_pct[i])}</td>
          <td style="padding:6px;text-align:right;font-weight:700;color:${marginColor}">${pct(hist.margin_pct[i])}</td>
        </tr>`;
      });
      html += '</table></div>';
      // Inline bar chart for margin
      html += '<div style="margin-top:16px"><div style="font-size:12px;color:#94a3b8;margin-bottom:8px">Margin % Trend (target: 15%)</div>';
      html += '<div style="display:flex;align-items:flex-end;gap:4px;height:80px">';
      const maxAbs = Math.max(...hist.margin_pct.map(v => Math.abs(v)), 15);
      hist.months.forEach((m, i) => {
        const v = hist.margin_pct[i];
        const h = Math.max(Math.abs(v) / maxAbs * 60, 2);
        const color = v >= 15 ? '#10b981' : v >= 0 ? '#fbbf24' : '#ef4444';
        const mLabel = new Date(m + '-15').toLocaleDateString('en-US',{month:'short'});
        html += `<div style="flex:1;display:flex;flex-direction:column;align-items:center">
          <div style="font-size:9px;color:${color};margin-bottom:2px">${v.toFixed(0)}%</div>
          <div style="width:100%;max-width:32px;height:${h}px;background:${color};border-radius:2px 2px 0 0"></div>
          <div style="font-size:9px;color:#64748b;margin-top:2px">${mLabel}</div>
        </div>`;
      });
      html += '</div></div>';
      html += '</div>';
    }

    // Path to 15%
    const p = d.path_to_target;
    if(p && p.gap_pct > 0 && p.recommendations.length > 0){
      html += `<div class="section"><h3>Path to ${pct(p.target_margin)} Margin</h3>`;
      html += `<div class="total-savings-banner">
        <div><div class="ts-label">Monthly gap to target</div><div class="ts-value">${fmt(p.gap_dollars)}/mo</div></div>
        <div><div class="ts-label">Potential savings if all targets met</div><div class="ts-value">${fmt(p.total_potential_savings)}/mo</div></div>
      </div>`;
      html += '<div class="path-grid">';
      p.recommendations.forEach((r, i) => {
        html += `<div class="path-card p${r.priority}">
          <div class="p-label">#${r.priority} &mdash; ${r.label}</div>
          <div class="p-pct">${pct(r.current_pct)} &rarr; ${pct(r.target_pct)}</div>
          <div class="p-savings">Save ${fmt(r.savings)}/mo</div>
          <div class="p-insight">${r.insight}</div>
        </div>`;
      });
      html += '</div></div>';
    } else if(p && p.gap_pct <= 0){
      html += `<div class="section"><h3>Target Achieved!</h3><p style="color:#6ee7b7;font-size:14px">Operating above the ${pct(p.target_margin)} margin target. Current margin: ${pct(p.current_margin)}.</p></div>`;
    }

    // Top Vendors Table — with subcategory column, sorted by budget status
    if(d.top_vendors && d.top_vendors.length){
      const sColors = {over_budget:'#ef4444',watch:'#fb923c',on_track:'#fbbf24',under_budget:'#10b981',informational:'#64748b',unknown:'#64748b'};
      const sLabels = {over_budget:'OVER',watch:'WATCH',on_track:'ON TRACK',under_budget:'ON TARGET',informational:'INFO',unknown:'—'};
      html += '<div class="section"><h3>Top Vendors This Month</h3>';
      html += '<p style="font-size:12px;color:#94a3b8;margin:-10px 0 14px">Sorted by budget status — vendors in over-budget subcategories appear first. These are your highest-impact negotiation targets.</p>';
      html += '<table class="vendor-table"><thead><tr><th>#</th><th>Vendor</th><th>Category</th><th>Subcategory</th><th>Status</th><th class="amt">Amount</th><th class="amt">Txns</th></tr></thead><tbody>';
      d.top_vendors.forEach((v, i) => {
        const subSt = v.subcategory_status || v.budget_status || 'unknown';
        const sc = sColors[subSt] || '#64748b';
        const sl = sLabels[subSt] || '—';
        const rowBg = (subSt === 'over_budget' || subSt === 'watch') ? 'rgba(239,68,68,0.06)' : 'transparent';
        const isActionable = subSt === 'over_budget' || subSt === 'watch';
        html += `<tr style="background:${rowBg}">
          <td>${i+1}</td>
          <td style="color:${isActionable ? '#fca5a5' : '#e2e8f0'};font-weight:${isActionable ? '600' : '400'}">${v.vendor}</td>
          <td>${v.budget_group || v.category}</td>
          <td style="font-size:12px;color:#94a3b8">${v.subcategory || '—'}</td>
          <td><span style="font-size:10px;font-weight:700;padding:2px 6px;border-radius:3px;background:${sc}22;color:${sc}">${sl}</span></td>
          <td class="amt">${fmt(v.amount)}</td>
          <td class="amt">${v.txns}</td></tr>`;
      });
      html += '</tbody></table></div>';
    }

    // Insights
    if(d.insights && d.insights.length){
      html += '<div class="section"><h3>Insights & Recommendations</h3>';
      d.insights.forEach(ins => {
        html += `<div class="insight-card sev-${ins.severity}">
          <div class="sev-badge ${ins.severity}">${ins.severity}</div>
          <div class="insight-text">${ins.text}</div>
        </div>`;
      });
      html += '</div>';
    }

    $('content').innerHTML = html;
  }

  $('btnAnalyze').addEventListener('click', loadBudget);
  loadBudget();
})();
</script>
</body>
</html>'''



def _event_roi_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 Event ROI</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#111;color:#e2e8f0;min-height:100vh}
.header{background:linear-gradient(135deg,#92400e,#d97706,#f59e0b);padding:24px 32px;text-align:center}
.header h1{font-size:24px;font-weight:700;color:#fff}
.header p{color:rgba(255,255,255,.85);font-size:13px;margin-top:4px}
.nav{display:flex;gap:4px;padding:8px 16px;background:#1a1a2e;flex-wrap:wrap;justify-content:center}
.nav a{color:#94a3b8;text-decoration:none;padding:6px 14px;border-radius:6px;font-size:13px;transition:.2s}
.nav a:hover{background:#334155;color:#e2e8f0}
.nav a.active{background:#d97706;color:#fff}
.container{max-width:1400px;margin:0 auto;padding:20px}
.filter-bar{display:flex;gap:12px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
.filter-bar label{color:#94a3b8;font-size:13px}
.filter-bar input[type="date"]{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 12px;border-radius:6px;font-size:14px}
.filter-bar button{background:#d97706;color:#fff;border:none;padding:8px 20px;border-radius:6px;font-size:14px;cursor:pointer;font-weight:600}
.filter-bar button:hover{background:#b45309}
.kpi-row{display:grid;grid-template-columns:repeat(7,1fr);gap:12px;margin-bottom:20px}
@media(max-width:1200px){.kpi-row{grid-template-columns:repeat(4,1fr)}}
@media(max-width:800px){.kpi-row{grid-template-columns:repeat(2,1fr)}}
.kpi-card{background:#1e293b;border-radius:10px;padding:16px;text-align:center;border:1px solid #334155}
.kpi-card .kpi-label{font-size:11px;color:#94a3b8;text-transform:uppercase;margin-bottom:4px}
.kpi-card .kpi-value{font-size:22px;font-weight:700;color:#e2e8f0}
.kpi-card .kpi-value.good{color:#10b981}
.kpi-card .kpi-value.warn{color:#f59e0b}
.kpi-card .kpi-value.bad{color:#ef4444}
.kpi-card .kpi-sub{font-size:10px;color:#64748b;margin-top:4px;line-height:1.3}
.section{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #334155}
.section h3{font-size:16px;font-weight:600;color:#e2e8f0;margin-bottom:16px}
.event-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:20px}
@media(max-width:900px){.event-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.event-grid{grid-template-columns:1fr}}
.ev-card{background:#1e293b;border-radius:12px;padding:20px;border:1px solid #334155;border-top:4px solid #d97706}
.ev-card .ev-name{font-size:15px;font-weight:700;color:#e2e8f0;margin-bottom:2px}
.ev-card .ev-dow{font-size:12px;color:#94a3b8;margin-bottom:12px}
.ev-card .ev-row{display:flex;justify-content:space-between;font-size:13px;padding:3px 0}
.ev-card .ev-row .lbl{color:#94a3b8}
.ev-card .ev-row .val{color:#e2e8f0;font-weight:600}
.ev-card .ev-roi{font-size:24px;font-weight:700;text-align:center;padding:10px 0;margin-top:8px;border-top:1px solid #334155}
.ev-card .ev-margin{font-size:13px;color:#94a3b8;text-align:center}
.bar-chart{display:flex;flex-direction:column;gap:12px}
.bar-row{display:flex;align-items:center;gap:10px}
.bar-label{width:140px;font-size:13px;color:#cbd5e1;text-align:right;flex-shrink:0}
.bar-container{flex:1;position:relative;height:28px}
.bar-fill{height:14px;border-radius:3px;position:absolute;top:0}
.bar-fill.revenue{background:#d97706}
.bar-fill.cost{background:#64748b;top:14px}
.bar-value{position:absolute;right:0;top:0;font-size:11px;color:#94a3b8;height:28px;display:flex;align-items:center;padding-left:4px}
.trend-table{width:100%;border-collapse:collapse;font-size:12px}
.trend-table th{text-align:center;padding:8px 6px;border-bottom:2px solid #334155;color:#94a3b8;font-size:11px}
.trend-table th:first-child{text-align:left}
.trend-table td{text-align:center;padding:6px;border-bottom:1px solid #1a1a2e;color:#e2e8f0}
.trend-table td:first-child{text-align:left;color:#cbd5e1}
.cost-section{margin-bottom:12px}
.cost-toggle{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:#111;border-radius:8px;cursor:pointer;border:1px solid #334155}
.cost-toggle:hover{border-color:#d97706}
.cost-toggle .ct-name{font-size:14px;font-weight:600;color:#e2e8f0}
.cost-toggle .ct-total{font-size:14px;color:#f59e0b;font-weight:600}
.cost-detail{display:none;padding:10px 14px;font-size:12px}
.cost-detail.open{display:block}
.cost-detail table{width:100%;border-collapse:collapse}
.cost-detail th{text-align:left;padding:4px 8px;color:#94a3b8;border-bottom:1px solid #334155;font-size:11px}
.cost-detail td{padding:4px 8px;color:#cbd5e1;border-bottom:1px solid #1e293b}
.cost-detail .amt{text-align:right;font-variant-numeric:tabular-nums}
.vendor-table{width:100%;border-collapse:collapse}
.vendor-table th{text-align:left;padding:10px 12px;border-bottom:2px solid #334155;color:#94a3b8;font-size:12px;text-transform:uppercase}
.vendor-table td{padding:8px 12px;border-bottom:1px solid #1e293b;font-size:13px;color:#e2e8f0}
.vendor-table tr:hover td{background:#334155}
.vendor-table .amt{text-align:right;font-variant-numeric:tabular-nums}
.insight-card{padding:14px 18px;border-radius:8px;margin-bottom:10px;border-left:4px solid #334155}
.insight-card.sev-critical{background:#7f1d1d22;border-left-color:#ef4444}
.insight-card.sev-warning{background:#7c2d1222;border-left-color:#f59e0b}
.insight-card.sev-info{background:#1e3a5f22;border-left-color:#3b82f6}
.insight-card.sev-good{background:#06563022;border-left-color:#10b981}
.insight-card .sev-badge{font-size:10px;font-weight:700;text-transform:uppercase;margin-bottom:4px}
.insight-card .sev-badge.critical{color:#fca5a5}
.insight-card .sev-badge.warning{color:#fbbf24}
.insight-card .sev-badge.info{color:#93c5fd}
.insight-card .sev-badge.good{color:#6ee7b7}
.insight-card .insight-text{font-size:13px;color:#cbd5e1;line-height:1.5}
.loading{text-align:center;padding:60px;color:#64748b}
.method-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
@media(max-width:800px){.method-grid{grid-template-columns:1fr}}
.method-card{background:#111;border-radius:8px;padding:16px;border:1px solid #334155}
.method-card .method-tier{font-size:10px;font-weight:700;text-transform:uppercase;color:#d97706;margin-bottom:4px}
.method-card .method-name{font-size:15px;font-weight:700;color:#e2e8f0;margin-bottom:6px}
.method-card .method-source{font-size:11px;color:#94a3b8;margin-bottom:8px;font-style:italic}
.method-card .method-desc{font-size:12px;color:#cbd5e1;line-height:1.5;margin-bottom:8px}
.method-card .method-adj{font-size:11px;color:#94a3b8}
</style>
</head>
<body>
<div class="header">
  <h1>LOV3 Event ROI</h1>
  <p>Per-event profitability analysis &mdash; recurring weekly events</p>
  <p style="max-width:720px;margin:8px auto 0;font-size:12px;color:rgba(255,255,255,.7);line-height:1.5">Which nights are earning their keep? This dashboard measures <strong>contribution margin</strong> &mdash; revenue minus the variable costs each event generates (entertainment, marketing, staffing). Fixed overhead like management salaries, rent, and COGS are excluded because they don&rsquo;t change if you add or cancel a night. Use this to identify underperforming events, evaluate promoter &amp; talent spend, and decide where to invest or cut.</p>
</div>
<div class="nav">
  <a href="/bank-review">Bank Review</a>
  <a href="/pnl">P&amp;L</a>
  <a href="/analysis">Analysis</a>
  <a href="/cash-recon">Cash Recon</a>
  <a href="/menu-mix">Menu Mix</a>
  <a href="/servers">Servers</a>
  <a href="/kitchen">Kitchen</a>
  <a href="/labor">Labor</a>
  <a href="/menu-eng">Menu Eng</a>
  <a href="/events">Events</a>
  <a href="/loyalty">Loyalty</a>
  <a href="/kpi-benchmarks">KPI</a>
  <a href="/budget">Budget</a>
  <a href="/event-roi" class="active">Event ROI</a>
</div>
<div class="container">
  <div class="filter-bar">
    <label>From:</label>
    <input type="date" id="startDate">
    <label>To:</label>
    <input type="date" id="endDate">
    <button id="btnAnalyze">Analyze</button>
  </div>
  <div id="content"><div class="loading">Loading event ROI data&hellip;</div></div>
</div>
<script>
(function(){
  const $ = id => document.getElementById(id);
  const fmt = n => n == null ? '--' : '$' + Math.abs(n).toLocaleString('en-US',{maximumFractionDigits:0});
  const fmtK = n => n == null ? '--' : (Math.abs(n) >= 1000 ? '$'+(Math.abs(n)/1000).toFixed(1)+'K' : '$'+Math.abs(n).toFixed(0));
  const pct = n => n == null ? '--' : n.toFixed(1) + '%';

  // Default: last 6 months
  const now = new Date();
  const sixAgo = new Date(now);
  sixAgo.setMonth(sixAgo.getMonth() - 6);
  $('startDate').value = sixAgo.toISOString().slice(0,10);
  $('endDate').value = now.toISOString().slice(0,10);

  function loadData(){
    const sd = $('startDate').value, ed = $('endDate').value;
    if(!sd || !ed) return;
    $('content').innerHTML = '<div class="loading">Loading event ROI data&hellip;</div>';
    fetch('/api/event-roi',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({start_date:sd, end_date:ed})
    })
    .then(r=>r.json())
    .then(d=>{
      if(d.error){$('content').innerHTML='<div class="loading" style="color:#fca5a5">Error: '+d.error+'</div>';return;}
      render(d);
    })
    .catch(e=>{$('content').innerHTML='<div class="loading" style="color:#fca5a5">'+e+'</div>';});
  }

  function roiColor(v){return v>=100?'#10b981':v>=50?'#f59e0b':'#ef4444'}
  function marginColor(v){return v>=50?'#10b981':v>=25?'#f59e0b':'#ef4444'}

  function render(d){
    const s = d.summary;
    let html = '';

    // KPI Summary Cards
    html += '<div class="kpi-row">';
    html += `<div class="kpi-card"><div class="kpi-label">Total Event Revenue</div><div class="kpi-value">${fmt(s.total_event_revenue)}</div></div>`;
    html += `<div class="kpi-card"><div class="kpi-label">Total Event Costs</div><div class="kpi-value">${fmt(s.total_event_costs)}</div><div class="kpi-sub">${fmt(s.total_direct_costs)} direct + ${fmt(s.total_shared_costs)} shared + ${fmt(s.total_labor_costs)} payroll + ${fmt(s.total_ops_labor_costs)} ops</div></div>`;
    html += `<div class="kpi-card"><div class="kpi-label">Net Contribution</div><div class="kpi-value" style="color:${s.total_net_contribution>=0?'#10b981':'#ef4444'}">${fmt(s.total_net_contribution)}</div></div>`;
    html += `<div class="kpi-card"><div class="kpi-label">Overall ROI</div><div class="kpi-value" style="color:${roiColor(s.overall_roi_pct)}">${pct(s.overall_roi_pct)}</div></div>`;
    html += `<div class="kpi-card"><div class="kpi-label">Overall Margin</div><div class="kpi-value" style="color:${marginColor(s.overall_margin_pct)}">${pct(s.overall_margin_pct)}</div></div>`;
    // Labor detail card
    const ld = s.labor_detail || {};
    const totalLabor = (s.total_labor_costs||0) + (s.total_ops_labor_costs||0);
    html += `<div class="kpi-card"><div class="kpi-label">Labor + Ops</div><div class="kpi-value">${fmt(totalLabor)}</div><div class="kpi-sub">Payroll ${fmt(s.total_labor_costs)} + Security/Staff ${fmt(s.total_ops_labor_costs)}</div></div>`;
    const unClass = s.unattributed_direct_costs > 0 ? 'warn' : 'good';
    html += `<div class="kpi-card"><div class="kpi-label">Unattributed Costs</div><div class="kpi-value ${unClass}">${fmt(s.unattributed_direct_costs)}</div></div>`;
    html += '</div>';

    // Event ROI Cards
    html += '<div class="event-grid">';
    d.events.forEach(ev => {
      const rc = roiColor(ev.roi.roi_pct);
      const borderColor = ev.roi.margin_pct >= 50 ? '#10b981' : ev.roi.margin_pct >= 25 ? '#d97706' : '#ef4444';
      html += `<div class="ev-card" style="border-top-color:${borderColor}">
        <div class="ev-name">${ev.label}</div>
        <div class="ev-dow">${ev.dow_name} &bull; ${ev.num_nights} nights &bull; ${ev.revenue.txn_count} txns</div>
        <div class="ev-row"><span class="lbl">Revenue</span><span class="val">${fmt(ev.revenue.adjusted_revenue)}</span></div>
        <div class="ev-row"><span class="lbl">Direct Costs</span><span class="val">${fmt(ev.costs.direct_costs)}</span></div>
        <div class="ev-row"><span class="lbl">Shared Costs</span><span class="val">${fmt(ev.costs.shared_costs)}</span></div>
        <div class="ev-row"><span class="lbl">Payroll (${ev.costs.labor_pct}%)</span><span class="val">${fmt(ev.costs.labor_costs)}</span></div>
        <div class="ev-row"><span class="lbl">Security/Staff</span><span class="val">${fmt(ev.costs.ops_labor_costs)}</span></div>
        <div class="ev-row"><span class="lbl">Net Contribution</span><span class="val" style="color:${ev.roi.net_contribution>=0?'#10b981':'#ef4444'}">${fmt(ev.roi.net_contribution)}</span></div>
        <div class="ev-row"><span class="lbl">Avg/Night</span><span class="val">${fmt(ev.revenue.avg_nightly)}</span></div>
        <div class="ev-roi" style="color:${rc}">${pct(ev.roi.roi_pct)} ROI</div>
        <div class="ev-margin">${pct(ev.roi.margin_pct)} margin &bull; ${fmt(ev.roi.cost_per_night)}/night cost</div>
      </div>`;
    });
    html += '</div>';

    // Revenue vs Cost Bar Chart
    const maxRev = Math.max(...d.events.map(e=>e.revenue.adjusted_revenue), 1);
    html += '<div class="section"><h3>Revenue vs Event Costs</h3><div class="bar-chart">';
    d.events.forEach(ev => {
      const revW = ev.revenue.adjusted_revenue / maxRev * 100;
      const costW = ev.costs.total_costs / maxRev * 100;
      html += `<div class="bar-row">
        <div class="bar-label">${ev.label}</div>
        <div class="bar-container">
          <div class="bar-fill revenue" style="width:${revW}%"></div>
          <div class="bar-fill cost" style="width:${costW}%"></div>
          <div class="bar-value">${fmtK(ev.revenue.adjusted_revenue)} rev / ${fmtK(ev.costs.total_costs)} cost</div>
        </div>
      </div>`;
    });
    html += '<div style="font-size:11px;color:#64748b;margin-top:8px"><span style="display:inline-block;width:12px;height:12px;background:#d97706;border-radius:2px;margin-right:4px;vertical-align:middle"></span>Revenue <span style="display:inline-block;width:12px;height:12px;background:#64748b;border-radius:2px;margin-left:12px;margin-right:4px;vertical-align:middle"></span>Event Costs</div>';
    html += '</div></div>';

    // Monthly Trend Table
    if(d.monthly_trend && d.monthly_trend.length){
      html += '<div class="section"><h3>Monthly Margin Trend</h3>';
      html += '<div style="overflow-x:auto"><table class="trend-table"><thead><tr><th>Month</th>';
      const eventKeys = d.events.map(e=>e.key);
      d.events.forEach(ev=>{html += `<th>${ev.label.split(' ').slice(0,2).join(' ')}</th>`});
      html += '</tr></thead><tbody>';
      d.monthly_trend.forEach(mt => {
        const mLabel = new Date(mt.month+'-15').toLocaleDateString('en-US',{month:'short',year:'2-digit'});
        html += `<tr><td>${mLabel}</td>`;
        eventKeys.forEach(ek => {
          const me = mt.events[ek];
          if(me && me.revenue > 0){
            const mc = marginColor(me.margin_pct);
            html += `<td style="color:${mc};font-weight:600">${pct(me.margin_pct)}</td>`;
          } else {
            html += '<td style="color:#334155">--</td>';
          }
        });
        html += '</tr>';
      });
      html += '</tbody></table></div></div>';
    }

    // Cost Breakdown Per Event (collapsible)
    html += '<div class="section"><h3>Cost Breakdown by Event</h3>';
    d.events.forEach((ev, idx) => {
      html += `<div class="cost-section">
        <div class="cost-toggle" onclick="this.nextElementSibling.classList.toggle('open')">
          <span class="ct-name">${ev.label}</span>
          <span class="ct-total">${fmt(ev.costs.total_costs)} total (${fmt(ev.costs.direct_costs)} direct + ${fmt(ev.costs.shared_costs)} shared + ${fmt(ev.costs.labor_costs)} payroll + ${fmt(ev.costs.ops_labor_costs)} ops)</span>
        </div>
        <div class="cost-detail">`;
      if(ev.costs.direct_vendors && ev.costs.direct_vendors.length){
        html += '<table><thead><tr><th>Vendor</th><th>Category</th><th class="amt">Amount</th><th class="amt">Txns</th></tr></thead><tbody>';
        ev.costs.direct_vendors.forEach(v => {
          html += `<tr><td>${v.vendor}</td><td>${v.category}</td><td class="amt">${fmt(v.amount)}</td><td class="amt">${v.txns}</td></tr>`;
        });
        html += '</tbody></table>';
      } else {
        html += '<div style="color:#64748b;padding:8px">No direct vendors mapped to this event</div>';
      }
      html += `<div style="margin-top:8px;font-size:12px;color:#94a3b8">Shared allocation: ${fmt(ev.costs.shared_costs)} (${ev.revenue.revenue_share_pct}% of shared pool based on revenue share)</div>`;
      html += `<div style="margin-top:4px;font-size:12px;color:#94a3b8">Payroll: ${fmt(ev.costs.labor_costs)} (${ev.costs.labor_pct}% of variable true labor) &bull; Security/Staffing: ${fmt(ev.costs.ops_labor_costs)} (${ev.costs.labor_pct}% of ops labor)</div>`;
      html += '</div></div>';
    });
    html += '</div>';

    // Unattributed Vendors
    if(d.unattributed_vendors && d.unattributed_vendors.length){
      html += '<div class="section"><h3>Unattributed Vendors <span style="font-size:12px;color:#f59e0b;font-weight:400">&mdash; map these to events for accurate ROI</span></h3>';
      html += '<table class="vendor-table"><thead><tr><th>#</th><th>Vendor</th><th>Category</th><th class="amt">Amount</th><th class="amt">Txns</th></tr></thead><tbody>';
      d.unattributed_vendors.forEach((v,i) => {
        html += `<tr><td>${i+1}</td><td>${v.vendor}</td><td>${v.category}</td><td class="amt">${fmt(v.amount)}</td><td class="amt">${v.txns}</td></tr>`;
      });
      html += '</tbody></table></div>';
    }

    // Insights
    if(d.insights && d.insights.length){
      html += '<div class="section"><h3>Insights & Recommendations</h3>';
      d.insights.forEach(ins => {
        html += `<div class="insight-card sev-${ins.severity}">
          <div class="sev-badge ${ins.severity}">${ins.severity}</div>
          <div class="insight-text">${ins.text}</div>
        </div>`;
      });
      html += '</div>';
    }

    // Cost Methodology
    html += `<div class="section">
      <h3>Cost Tier Methodology</h3>
      <p style="font-size:13px;color:#94a3b8;margin-bottom:16px">How costs are attributed to each event night. Revenue = net_sales + (gratuity &times; 35% house-retained).</p>
      <div class="method-grid">
        <div class="method-card">
          <div class="method-tier">Tier 1</div>
          <div class="method-name">Direct Costs</div>
          <div class="method-source">Bank &mdash; PMG Artist, Entertainment, Promoter Payout, Pay-Per-View</div>
          <div class="method-desc">Vendor payments mapped to specific events via vendor keyword matching. Multi-night vendors split 50/50 evenly across assigned nights.</div>
          <div class="method-adj"><span style="color:#10b981">&#10003;</span> No pass-through adjustment needed</div>
        </div>
        <div class="method-card">
          <div class="method-tier">Tier 2</div>
          <div class="method-name">Shared Costs</div>
          <div class="method-source">Bank &mdash; Social Media Marketing, Event Flyers, Digital Ads, Event Expense</div>
          <div class="method-desc">Allocated proportionally by each event&rsquo;s share of total monthly revenue. Higher-revenue nights absorb more shared cost.</div>
          <div class="method-adj"><span style="color:#10b981">&#10003;</span> No pass-through adjustment needed</div>
        </div>
        <div class="method-card">
          <div class="method-tier">Tier 3</div>
          <div class="method-name">Payroll Labor</div>
          <div class="method-source">Bank &mdash; categories matching &ldquo;labor&rdquo; / &ldquo;payroll&rdquo; (excludes security &amp; contract labor)</div>
          <div class="method-desc">True Labor = Gross Payroll &minus; Tips (100%) &minus; Gratuity (&times; 65% staff share) &minus; Fixed Overhead (mgmt $20K/period + 1099 $3.5K/period). Variable remainder allocated by DOW staffing %.</div>
          <div class="method-adj"><span style="color:#f59e0b">&#9888;</span> Tips &amp; gratuity pass-through stripped from gross payroll before allocation</div>
        </div>
        <div class="method-card">
          <div class="method-tier">Tier 4</div>
          <div class="method-name">Security &amp; Staffing</div>
          <div class="method-source">Bank &mdash; Security Services (Lewis Security), Contract Labor (ABC Staffing, Alberto Batz)</div>
          <div class="method-desc">Vendor invoice payments allocated directly by DOW staffing %. These are straight vendor costs with no payroll pass-through embedded.</div>
          <div class="method-adj"><span style="color:#10b981">&#10003;</span> No pass-through adjustment needed (vendor payments)</div>
        </div>
      </div>
      <div style="margin-top:16px;padding:14px;background:#111;border-radius:8px;border:1px solid #334155">
        <div style="font-size:13px;font-weight:600;color:#d97706;margin-bottom:8px">DOW Staffing Allocation %</div>
        <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:13px;color:#cbd5e1">
          <span>Tue <b style="color:#f59e0b">6.5%</b></span>
          <span>Wed <b style="color:#f59e0b">9.6%</b></span>
          <span>Thu <b style="color:#f59e0b">15.0%</b></span>
          <span>Fri <b style="color:#f59e0b">30.9%</b></span>
          <span>Sat <b style="color:#f59e0b">28.0%</b></span>
          <span>Sun <b style="color:#f59e0b">10.0%</b></span>
        </div>
        <div style="margin-top:8px;font-size:11px;color:#64748b">Applied to Payroll (Tier 3) and Security/Staffing (Tier 4). Monday is dark (closed). Based on management staffing report.</div>
      </div>
      <div style="margin-top:12px;padding:14px;background:#111;border-radius:8px;border:1px solid #334155">
        <div style="font-size:13px;font-weight:600;color:#d97706;margin-bottom:8px">What&rsquo;s NOT Included</div>
        <div style="font-size:12px;color:#94a3b8;line-height:1.6">
          Rent, utilities, insurance, COGS/food costs, liquor costs, permits, and other fixed overhead are excluded from event ROI.
          These are tracked in the <a href="/budget" style="color:#d97706">Budget</a> dashboard. Event ROI measures the incremental profitability of each night&rsquo;s programming.
        </div>
      </div>
    </div>`;

    $('content').innerHTML = html;
  }

  $('btnAnalyze').addEventListener('click', loadData);
  loadData();
})();
</script>
</body>
</html>'''



def _flash_report_html() -> str:
    """Daily Flash Report dashboard — KPIs, server leaderboard, margins, cash gap."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Daily Flash Report</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f172a;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
.nav-bar{display:flex;gap:0;background:#1a1a2e;padding:0 16px;flex-wrap:wrap}
.nav-bar a{color:#94a3b8;text-decoration:none;padding:12px 16px;font-size:0.82rem;font-weight:500;transition:all 0.15s;border-bottom:2px solid transparent;white-space:nowrap}
.nav-bar a:hover{color:#fff;background:rgba(255,255,255,0.05)}
.nav-bar a.active{color:#fff;border-bottom-color:#f59e0b;background:rgba(245,158,11,0.1)}
.container{max-width:1200px;margin:0 auto;padding:24px}
.header{text-align:center;padding:24px 0;background:linear-gradient(135deg,#f59e0b22,#d9770622);border-radius:12px;margin-bottom:24px}
.header h1{font-size:1.6rem;color:#f59e0b}
.header p{color:#94a3b8;font-size:0.9rem;margin-top:4px}
.date-picker{display:flex;justify-content:center;gap:12px;margin:16px 0}
.date-picker input{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 16px;border-radius:8px;font-size:0.9rem}
.date-picker button{background:#f59e0b;color:#000;border:none;padding:8px 24px;border-radius:8px;font-weight:600;cursor:pointer}
.date-picker button:hover{background:#d97706}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.kpi{background:#1e293b;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.05em}
.kpi .value{font-size:1.8rem;font-weight:700;margin:8px 0}
.kpi .change{font-size:0.8rem;font-weight:600}
.kpi .change.up{color:#22c55e}
.kpi .change.down{color:#ef4444}
.section{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.section h2{font-size:1rem;color:#f59e0b;margin-bottom:12px}
.server-row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #334155}
.server-row:last-child{border:none}
.server-rank{color:#94a3b8;width:30px}
.server-name{flex:1;font-weight:500}
.server-rev{color:#22c55e;font-weight:600}
.margin-row{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid #334155}
.margin-row:last-child{border:none}
.margin-label{color:#94a3b8}
.margin-val{font-weight:600}
.margin-val.good{color:#22c55e}
.margin-val.warn{color:#f59e0b}
.margin-val.bad{color:#ef4444}
.cash-row{display:flex;justify-content:space-between;padding:10px 0}
.loading{text-align:center;padding:40px;color:#94a3b8}
@media(max-width:768px){.kpi-grid{grid-template-columns:1fr 1fr}.container{padding:12px}}
</style>
</head>
<body>
<div class="nav-bar">
<a href="/bank-review">Bank Review</a><a href="/pnl">P&amp;L</a><a href="/analysis">Analysis</a>
<a href="/cash-recon">Cash Recon</a><a href="/menu-mix">Menu Mix</a><a href="/servers">Servers</a>
<a href="/kitchen">Kitchen</a><a href="/labor">Labor</a><a href="/menu-eng">Menu Eng</a>
<a href="/events">Events</a><a href="/loyalty">Loyalty</a><a href="/kpi-benchmarks">KPI</a>
<a href="/budget">Budget</a><a href="/event-roi">Event ROI</a>
<a href="/flash" class="active">Flash</a>
</div>
<div class="container">
<div class="header">
<h1>🍴 Daily Flash Report</h1>
<p id="subtitle">Loading...</p>
<div class="date-picker">
<input type="date" id="dateInput">
<button onclick="loadReport()">Load</button>
<button onclick="sendReport()" style="background:#3b82f6">📤 Send Slack + Email</button>
</div>
</div>

<div id="content"><div class="loading">Loading flash report...</div></div>
</div>

<script>
const API = '/api/flash-report';
let currentData = null;

function fmt(n){return new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(n)}
function pct(n){return n.toFixed(1)+'%'}
function cls(val,goodMax,warnMax){return val<=goodMax?'good':val<=warnMax?'warn':'bad'}
function clsMin(val,goodMin,warnMin){return val>=goodMin?'good':val>=warnMin?'warn':'bad'}

async function loadReport(d){
  const date = d || document.getElementById('dateInput').value;
  const body = date ? JSON.stringify({date}) : '{}';
  try {
    const r = await fetch(API,{method:'POST',headers:{'Content-Type':'application/json'},body});
    const data = await r.json();
    if(data.error){document.getElementById('content').innerHTML=`<div class="loading">Error: ${data.error}</div>`;return}
    currentData = data;
    render(data);
  } catch(e){document.getElementById('content').innerHTML=`<div class="loading">Failed to load: ${e.message}</div>`}
}

async function sendReport(){
  if(!currentData)return;
  const body = JSON.stringify({date:currentData.date});
  await fetch(API+'?send=true',{method:'POST',headers:{'Content-Type':'application/json'},body});
  alert('Flash report sent to Slack + Email!');
}

function render(d){
  const c = d.comparison;
  const arrow = c.revenue_change_pct >= 0 ? '↑' : '↓';
  const chgCls = c.revenue_change_pct >= 0 ? 'up' : 'down';

  document.getElementById('subtitle').textContent = `${d.day_name} ${d.date}`;
  document.getElementById('dateInput').value = d.date;

  let servers = '';
  (d.top_servers||[]).forEach((s,i) => {
    servers += `<div class="server-row"><span class="server-rank">#${i+1}</span><span class="server-name">${s.server}</span><span class="server-rev">${fmt(s.revenue)}</span></div>`;
  });

  const m = d.margins;
  const cash = d.cash;

  document.getElementById('content').innerHTML = `
    <div class="kpi-grid">
      <div class="kpi"><div class="label">💰 Revenue</div><div class="value">${fmt(d.revenue)}</div><div class="change ${chgCls}">${arrow}${Math.abs(c.revenue_change_pct).toFixed(0)}% vs last ${d.day_name.slice(0,3)}</div></div>
      <div class="kpi"><div class="label">📋 Orders</div><div class="value">${d.orders}</div><div class="change" style="color:#94a3b8">Prior: ${c.prior_orders}</div></div>
      <div class="kpi"><div class="label">👥 Guests</div><div class="value">${d.guests}</div><div class="change" style="color:#94a3b8">Prior: ${c.prior_guests}</div></div>
      <div class="kpi"><div class="label">💳 Avg Check</div><div class="value">${fmt(d.avg_check)}</div><div class="change" style="color:#94a3b8">Tips: ${fmt(d.tips)}</div></div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
      <div class="section">
        <h2>🏆 Top Servers</h2>
        ${servers || '<div style="color:#94a3b8">No server data</div>'}
      </div>
      <div class="section">
        <h2>📊 Margins</h2>
        <div class="margin-row"><span class="margin-label">COGS %</span><span class="margin-val ${cls(m.cogs_pct,30,35)}">${pct(m.cogs_pct)}</span></div>
        <div class="margin-row"><span class="margin-label">True Labor %</span><span class="margin-val ${cls(m.labor_pct,28,33)}">${pct(m.labor_pct)}</span></div>
        <div class="margin-row"><span class="margin-label">Net Margin</span><span class="margin-val ${clsMin(m.net_pct,12,5)}">${pct(m.net_pct)}</span></div>
        <div class="margin-row"><span class="margin-label">Adj Revenue</span><span class="margin-val" style="color:#e2e8f0">${fmt(m.adj_revenue||0)}</span></div>
      </div>
    </div>

    <div class="section">
      <h2>💵 Cash Reconciliation</h2>
      <div class="cash-row"><span>POS Cash Collected</span><span style="color:#22c55e;font-weight:600">${fmt(cash.collected)}</span></div>
      <div class="cash-row"><span>Bank Cash Deposited</span><span style="color:#3b82f6;font-weight:600">${fmt(cash.deposited)}</span></div>
      <div class="cash-row"><span>Gap</span><span class="margin-val ${Math.abs(cash.gap)<100?'good':Math.abs(cash.gap)<500?'warn':'bad'}">${fmt(cash.gap)}</span></div>
    </div>
  `;
}

// Auto-load yesterday on page load
loadReport();
</script>
</body>
</html>"""


def _vendor_tracker_html() -> str:
    """Vendor Spend Tracker dashboard — top vendors, trends, anomalies."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LOV3 Vendor Spend Tracker</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f172a;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}
.nav-bar{display:flex;gap:0;background:#1a1a2e;padding:0 16px;flex-wrap:wrap}
.nav-bar a{color:#94a3b8;text-decoration:none;padding:12px 16px;font-size:0.82rem;font-weight:500;transition:all 0.15s;border-bottom:2px solid transparent;white-space:nowrap}
.nav-bar a:hover{color:#fff;background:rgba(255,255,255,0.05)}
.nav-bar a.active{color:#fff;border-bottom-color:#8b5cf6;background:rgba(139,92,246,0.1)}
.container{max-width:1200px;margin:0 auto;padding:24px}
.header{text-align:center;padding:24px 0;background:linear-gradient(135deg,#8b5cf622,#6d28d922);border-radius:12px;margin-bottom:24px}
.header h1{font-size:1.6rem;color:#a78bfa}
.header p{color:#94a3b8;font-size:0.9rem;margin-top:4px}
.filter-bar{display:flex;justify-content:center;gap:12px;margin:16px 0;flex-wrap:wrap}
.filter-bar input,.filter-bar select{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 16px;border-radius:8px;font-size:0.9rem}
.filter-bar button{background:#8b5cf6;color:#fff;border:none;padding:8px 24px;border-radius:8px;font-weight:600;cursor:pointer}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.kpi{background:#1e293b;border-radius:12px;padding:20px;text-align:center}
.kpi .label{font-size:0.75rem;color:#94a3b8;text-transform:uppercase;letter-spacing:0.05em}
.kpi .value{font-size:1.8rem;font-weight:700;margin:8px 0;color:#a78bfa}
.section{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.section h2{font-size:1rem;color:#a78bfa;margin-bottom:12px}
table{width:100%;border-collapse:collapse;font-size:0.85rem}
th{text-align:left;color:#94a3b8;padding:8px 12px;border-bottom:1px solid #334155;font-weight:500;cursor:pointer}
th:hover{color:#a78bfa}
td{padding:8px 12px;border-bottom:1px solid #1e293b}
tr:hover{background:#1e293b88}
.spend-bar{height:6px;background:#334155;border-radius:3px;overflow:hidden;margin-top:4px}
.spend-bar .fill{height:100%;background:linear-gradient(90deg,#8b5cf6,#a78bfa);border-radius:3px}
.anomaly{background:#7f1d1d22;border:1px solid #991b1b;border-radius:8px;padding:12px;margin-bottom:8px}
.anomaly.medium{background:#78350f22;border-color:#92400e}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.7rem;font-weight:600}
.badge.high{background:#991b1b;color:#fca5a5}
.badge.medium{background:#92400e;color:#fcd34d}
.loading{text-align:center;padding:40px;color:#94a3b8}
@media(max-width:768px){.kpi-grid{grid-template-columns:1fr 1fr}.container{padding:12px}}
</style>
</head>
<body>
<div class="nav-bar">
<a href="/bank-review">Bank Review</a><a href="/pnl">P&amp;L</a><a href="/analysis">Analysis</a>
<a href="/cash-recon">Cash Recon</a><a href="/menu-mix">Menu Mix</a><a href="/servers">Servers</a>
<a href="/kitchen">Kitchen</a><a href="/labor">Labor</a><a href="/menu-eng">Menu Eng</a>
<a href="/events">Events</a><a href="/loyalty">Loyalty</a><a href="/kpi-benchmarks">KPI</a>
<a href="/budget">Budget</a><a href="/event-roi">Event ROI</a><a href="/flash">Flash</a>
<a href="/vendors" class="active">Vendors</a>
</div>
<div class="container">
<div class="header">
<h1>🏢 Vendor Spend Tracker</h1>
<p>Top vendors by spend, month-over-month trends, and cost anomalies</p>
<div class="filter-bar">
<input type="date" id="startDate" value="2025-09-01">
<input type="date" id="endDate" value="">
<button onclick="loadData()">Analyze</button>
</div>
</div>

<div id="content"><div class="loading">Loading vendor data...</div></div>
</div>

<script>
const API='/api/vendor-tracker';
document.getElementById('endDate').value=new Date().toISOString().split('T')[0];

function fmt(n){return new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(n)}

async function loadData(){
  const s=document.getElementById('startDate').value;
  const e=document.getElementById('endDate').value;
  try{
    const r=await fetch(API,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({start_date:s,end_date:e})});
    const d=await r.json();
    if(d.error){document.getElementById('content').innerHTML=`<div class="loading">Error: ${d.error}</div>`;return}
    render(d);
  }catch(e){document.getElementById('content').innerHTML=`<div class="loading">Failed: ${e.message}</div>`}
}

function render(d){
  const k=d.kpis;const c=d.concentration;
  const maxSpend=d.top_vendors.length?d.top_vendors[0].total_spend:1;

  let vendorRows='';
  d.top_vendors.forEach((v,i)=>{
    const pct=(v.total_spend/maxSpend*100).toFixed(0);
    vendorRows+=`<tr>
      <td style="color:#94a3b8">${i+1}</td>
      <td><strong>${v.vendor}</strong><div class="spend-bar"><div class="fill" style="width:${pct}%"></div></div></td>
      <td style="font-size:0.75rem;color:#94a3b8">${v.category_section}</td>
      <td style="color:#a78bfa;font-weight:600;text-align:right">${fmt(v.total_spend)}</td>
      <td style="text-align:right">${v.txn_count}</td>
      <td style="text-align:right">${fmt(v.avg_per_txn)}</td>
      <td style="text-align:right;color:#94a3b8">${v.active_months}mo</td>
    </tr>`;
  });

  let anomalyCards='';
  if(d.anomalies.length===0)anomalyCards='<div style="color:#94a3b8;padding:12px">No anomalies detected — vendor spending is stable.</div>';
  d.anomalies.forEach(a=>{
    anomalyCards+=`<div class="anomaly ${a.severity}">
      <span class="badge ${a.severity}">${a.severity.toUpperCase()}</span>
      <strong>${a.vendor}</strong> — ${a.month}: ${fmt(a.current_spend)} (was ${fmt(a.prior_spend)}, <strong>+${a.change_pct}%</strong>)
    </div>`;
  });

  let catRows='';
  d.category_breakdown.forEach(c=>{
    catRows+=`<tr><td>${c.section}</td><td style="color:#a78bfa;font-weight:600;text-align:right">${fmt(c.total_spend)}</td><td style="text-align:right">${c.vendor_count}</td><td style="text-align:right">${c.txn_count}</td></tr>`;
  });

  document.getElementById('content').innerHTML=`
    <div class="kpi-grid">
      <div class="kpi"><div class="label">Total Spend</div><div class="value">${fmt(k.total_spend)}</div></div>
      <div class="kpi"><div class="label">Active Vendors</div><div class="value">${k.total_vendors}</div></div>
      <div class="kpi"><div class="label">Top 5 Concentration</div><div class="value">${c.top_5_pct}%</div></div>
      <div class="kpi"><div class="label">⚠️ Anomalies</div><div class="value" style="color:${k.anomaly_count>0?'#f59e0b':'#22c55e'}">${k.anomaly_count}</div></div>
    </div>

    ${d.anomalies.length?`<div class="section"><h2>⚠️ Spend Anomalies (>25% MoM increase)</h2>${anomalyCards}</div>`:''}

    <div class="section">
      <h2>🏢 Top ${d.top_vendors.length} Vendors by Spend</h2>
      <table>
        <tr><th>#</th><th>Vendor</th><th>Category</th><th style="text-align:right">Total Spend</th><th style="text-align:right">Txns</th><th style="text-align:right">Avg/Txn</th><th style="text-align:right">Active</th></tr>
        ${vendorRows}
      </table>
    </div>

    <div class="section">
      <h2>📊 Spend by Category</h2>
      <table>
        <tr><th>Category</th><th style="text-align:right">Total Spend</th><th style="text-align:right">Vendors</th><th style="text-align:right">Transactions</th></tr>
        ${catRows}
      </table>
    </div>

    <div class="section" style="font-size:0.85rem;color:#94a3b8">
      <h2>📈 Vendor Concentration</h2>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-top:12px">
        <div>Top 5 vendors: <strong style="color:#a78bfa">${c.top_5_pct}%</strong> of spend</div>
        <div>Top 10 vendors: <strong style="color:#a78bfa">${c.top_10_pct}%</strong> of spend</div>
        <div>Top 20 vendors: <strong style="color:#a78bfa">${c.top_20_pct}%</strong> of spend</div>
      </div>
    </div>
  `;
}

loadData();
</script>
</body>
</html>"""
