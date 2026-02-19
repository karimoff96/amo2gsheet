/**
 * ============================================================
 *  amoCRM <-> Google Sheets two-way sync
 *
 *  FLOW:
 *  1. Lead reaches "NOMERATSIYALANMAGAN ZAKAZ" in amoCRM
 *     -> webhook fires -> row is written to Sheet1
 *  2. User edits the "status" dropdown in the sheet
 *     -> onEdit trigger fires -> amoCRM lead status is updated
 * ============================================================
 *
 *  SETUP ORDER:
 *  Step 1 - Paste this script in Extensions -> Apps Script, save.
 *  Step 2 - Run  setupCredentials()
 *  Step 3 - Run  exchangeCode("https://ya.ru/?code=...")  with the redirect URL.
 *  Step 4 - Run  fetchAndShowStructure()  to get pipeline / status IDs.
 *  Step 5 - Fill TRIGGER_STATUS_ID, PIPELINE_ID, DROPDOWN_STATUS_MAP below.
 *  Step 6 - Deploy as Web App (Execute as: Me, Access: Anyone).
 *  Step 7 - In amoCRM -> Settings -> Webhooks, add the Web App URL.
 *  Step 8 - Run  installOnEditTrigger()  once.
 * ============================================================
 */

// 
//  CONFIG  (fill in after running Steps 3-4)
// 

var SHEET_NAME = "Sheet1";

// amoCRM subdomain (part before .amocrm.ru)
var AMO_SUBDOMAIN = "bioflextest";

// Status ID that triggers copying the lead to the sheet.
// Run fetchAndShowStructure() to find this ID.
var TRIGGER_STATUS_ID = 0; // <- replace with actual ID

// Map of status NAMES shown in Sheet dropdown -> amoCRM status IDs.
// Run fetchAndShowStructure() to find the IDs.
var DROPDOWN_STATUS_MAP = {
  "Успешно": 0,  // <- replace 0 with actual amoCRM status ID
  "Отказ":   0,  // <- replace 0 with actual amoCRM status ID
};

// Pipeline ID that contains the statuses above.
var PIPELINE_ID = 0; // <- replace with actual pipeline ID

// 
//  COLUMNS  (must match row 1 of Sheet1)
// ──────────────

var COLUMNS = [
  "ID",               // col A  - amoCRM lead ID (row key, do not remove)
  "Ф.И.О.",           // col B
  "Контактный номер", // col C
  "Компания",         // col D
  "№",                // col E
  "Дата Заказа",      // col F
  "Дата доставки",    // col G
  "Оператор (ПИН)",   // col H
  "Bo'lim",           // col I
  "Товар1",           // col J
  "кол-во1",          // col K
  "Товар2",           // col L
  "кол-во2",          // col M
  "Товар3",           // col N
  "кол-во3",          // col O
  "Сумма",            // col P
  "Регион",           // col Q
  "Адрес",            // col R
  "статус",           // col S  <- user edits this dropdown
  "Логистика",        // col T
  "Контакт",          // col U
  "Источник",         // col V
];

var STATUS_COL_INDEX = COLUMNS.indexOf("статус") + 1; // 1-based
var ID_COL_INDEX     = COLUMNS.indexOf("ID") + 1;     // 1-based

// ─────────────────────────────────────────────
//  STEP 2-3: ONE-TIME CREDENTIAL SETUP
// ─────────────────────────────────────────────

/**
 * Run once from Apps Script editor. Stores credentials and shows the auth URL.
 */
function setupCredentials() {
  var props = PropertiesService.getScriptProperties();
  props.setProperties({
    "AMO_CLIENT_ID":     "5abd5b32-2ccd-41cc-91ba-a76ef6080cb1",
    "AMO_CLIENT_SECRET": "L88UTVe7P03XpZg1txBT8l5Qo8g7kIqmOeu66zi28SuavGBC39xkIpD60zhXScbq",
    "AMO_REDIRECT_URI":  "https://ya.ru",
  });

  var authUrl = "https://www.amocrm.ru/oauth"
    + "?client_id=5abd5b32-2ccd-41cc-91ba-a76ef6080cb1"
    + "&response_type=code"
    + "&redirect_uri=" + encodeURIComponent("https://ya.ru")
    + "&state=setup";

  var html = HtmlService.createHtmlOutput(
    "<h3>amoCRM Authorization</h3>" +
    "<p>1. <a href='" + authUrl + "' target='_blank'><b>Click here to authorize</b></a></p>" +
    "<p>2. Log in and click <b>Разрешить (Allow)</b></p>" +
    "<p>3. You'll land on <b>ya.ru</b>. Copy the full URL from the address bar.</p>" +
    "<p>4. Back in Apps Script, run:<br><code>exchangeCode(\"paste-the-ya.ru-url-here\")</code></p>"
  ).setWidth(480).setHeight(260);
  SpreadsheetApp.getUi().showModalDialog(html, "Step 2: Authorize amoCRM");
}

/**
 * Run after setupCredentials(). Pass the full ya.ru redirect URL.
 * Example: exchangeCode("https://ya.ru/?code=eyJ0eXAi...")
 */
function exchangeCode(redirectUrl) {
  var code = redirectUrl;
  if (redirectUrl.indexOf("code=") !== -1) {
    code = redirectUrl.split("code=")[1].split("&")[0];
  }

  var props = PropertiesService.getScriptProperties();
  var r = UrlFetchApp.fetch(
    "https://" + AMO_SUBDOMAIN + ".amocrm.ru/oauth2/access_token",
    {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify({
        client_id:     props.getProperty("AMO_CLIENT_ID"),
        client_secret: props.getProperty("AMO_CLIENT_SECRET"),
        grant_type:    "authorization_code",
        code:          code,
        redirect_uri:  props.getProperty("AMO_REDIRECT_URI"),
      }),
      muteHttpExceptions: true,
    }
  );

  var result = JSON.parse(r.getContentText());
  if (result.access_token) {
    props.setProperty("AMO_ACCESS_TOKEN",  result.access_token);
    props.setProperty("AMO_REFRESH_TOKEN", result.refresh_token);
    SpreadsheetApp.getUi().alert("✓ Tokens saved!\n\nNow run fetchAndShowStructure() to get your pipeline/status IDs.");
  } else {
    SpreadsheetApp.getUi().alert("✗ Error:\n" + r.getContentText());
  }
}

// ─────────────────────────────────────────────
//  STEP 4: FETCH PIPELINE / STATUS STRUCTURE
// ─────────────────────────────────────────────

/**
 * Fetches all pipelines + statuses and writes them to a "Structure" sheet.
 * Use the IDs shown to fill TRIGGER_STATUS_ID, PIPELINE_ID, DROPDOWN_STATUS_MAP.
 */
function fetchAndShowStructure() {
  var data = amoGet("/api/v4/leads/pipelines?with=statuses&limit=250");
  if (!data || !data._embedded) {
    SpreadsheetApp.getUi().alert("Failed to fetch structure. Check token.");
    return;
  }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName("Structure") || ss.insertSheet("Structure");
  sh.clearContents();
  sh.getRange(1,1).setValue("Pipeline");
  sh.getRange(1,2).setValue("Pipeline ID");
  sh.getRange(1,3).setValue("Status Name");
  sh.getRange(1,4).setValue("Status ID");
  sh.getRange(1, 1, 1, 4).setFontWeight("bold");

  var row = 2;
  data._embedded.pipelines.forEach(function(p) {
    if (p._embedded && p._embedded.statuses) {
      p._embedded.statuses.forEach(function(s) {
        sh.getRange(row, 1).setValue(p.name);
        sh.getRange(row, 2).setValue(p.id);
        sh.getRange(row, 3).setValue(s.name);
        sh.getRange(row, 4).setValue(s.id);
        row++;
      });
    }
  });
  sh.autoResizeColumns(1, 4);
  ss.setActiveSheet(sh);
  SpreadsheetApp.getUi().alert(
    "✓ Done! Check the 'Structure' sheet.\n\n" +
    "Copy the IDs into the CONFIG section at the top of this script:\n" +
    "  - TRIGGER_STATUS_ID\n  - PIPELINE_ID\n  - DROPDOWN_STATUS_MAP"
  );
}

// ─────────────────────────────────────────────
//  STEP 8: INSTALL ONEDIT TRIGGER
// ─────────────────────────────────────────────

/**
 * Run once to register the trigger and add dropdowns to the status column.
 */
function installOnEditTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === "onStatusEdit") ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger("onStatusEdit")
    .forSpreadsheet(SpreadsheetApp.getActiveSpreadsheet())
    .onEdit()
    .create();
  addStatusDropdown();
  SpreadsheetApp.getUi().alert("✓ Trigger installed and dropdowns added to the status column.");
}

function addStatusDropdown() {
  var sheet   = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  var lastRow = Math.max(sheet.getLastRow(), 1000);
  var options = Object.keys(DROPDOWN_STATUS_MAP);
  if (options.length === 0) return;
  var rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(options, true)
    .setAllowInvalid(false)
    .build();
  sheet.getRange(2, STATUS_COL_INDEX, lastRow - 1, 1).setDataValidation(rule);
}

// ─────────────────────────────────────────────
//  SHEET -> amoCRM  (onEdit trigger)
// ─────────────────────────────────────────────

function onStatusEdit(e) {
  var range = e.range;
  var sheet = range.getSheet();

  if (sheet.getName()   !== SHEET_NAME)         return;
  if (range.getColumn() !== STATUS_COL_INDEX)    return;
  if (range.getRow()    <= 1)                    return;

  var newStatusName = range.getValue();
  var newStatusId   = DROPDOWN_STATUS_MAP[newStatusName];
  if (!newStatusId) return;

  var leadId = sheet.getRange(range.getRow(), ID_COL_INDEX).getValue();
  if (!leadId) return;

  var result = amoPatch("/api/v4/leads/" + leadId, {
    status_id:   newStatusId,
    pipeline_id: PIPELINE_ID,
  });

  Logger.log(result && result.id
    ? "✓ Lead " + leadId + " -> " + newStatusName
    : "✗ Failed: " + JSON.stringify(result));
}

// ─────────────────────────────────────────────
//  amoCRM -> SHEET  (webhook receiver)
// ─────────────────────────────────────────────

function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({ status: "ok" }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    var raw  = (e.postData && e.postData.contents) ? e.postData.contents : "";
    var data = {};
    try { data = JSON.parse(raw); } catch (_) { data = parseFormEncoded(raw); }

    var leads = extractLeads(data);
    var ss    = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
    ensureHeaders(sheet);

    var written = 0;
    leads.forEach(function(lead) {
      var statusId = parseInt(lead.status_id || 0, 10);
      if (TRIGGER_STATUS_ID !== 0 && statusId !== TRIGGER_STATUS_ID) return;

      var leadId   = String(lead.id || "");
      var row      = buildRow(lead);
      var existing = findRow(sheet, leadId);

      if (existing > 0) {
        sheet.getRange(existing, 1, 1, row.length).setValues([row]);
      } else {
        sheet.appendRow(row);
        applyDropdownToRow(sheet, sheet.getLastRow());
      }
      written++;
    });

    return ContentService
      .createTextOutput(JSON.stringify({ status: "ok", written: written }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    Logger.log("doPost error: " + err);
    return ContentService
      .createTextOutput(JSON.stringify({ status: "error", message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function applyDropdownToRow(sheet, rowNum) {
  var options = Object.keys(DROPDOWN_STATUS_MAP);
  if (options.length === 0) return;
  var rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(options, true)
    .setAllowInvalid(false)
    .build();
  sheet.getRange(rowNum, STATUS_COL_INDEX).setDataValidation(rule);
}

// 
//  DATA HELPERS
// 

function parseFormEncoded(body) {
  var result = {};
  (body || "").split("&").forEach(function(pair) {
    var p = pair.split("=");
    if (p.length === 2) result[decodeURIComponent(p[0])] = decodeURIComponent(p[1].replace(/\+/g," "));
  });
  return result;
}

function extractLeads(data) {
  if (data._embedded && data._embedded.leads) return data._embedded.leads;
  var map = {};
  Object.keys(data).forEach(function(key) {
    var m = key.match(/^leads\[(?:add|update|status)\]\[(\d+)\]\[(.+)\]$/);
    if (m) { map[m[1]] = map[m[1]] || {}; map[m[1]][m[2]] = data[key]; }
  });
  return Object.values(map);
}

function buildRow(lead) {
  var m = {};
  m["ID"]     = lead.id    || "";
  m["Сумма"]  = lead.price || "";
  m["статус"] = lead.status_id || "";
  if (lead.name)          m["Ф.И.О."]           = lead.name;
  if (lead.contact_name)  m["Ф.И.О."]           = m["Ф.И.О."] || lead.contact_name;
  if (lead.contact_phone) m["Контактный номер"]  = lead.contact_phone;

  // Custom fields (JSON)
  if (lead.custom_fields_values) {
    lead.custom_fields_values.forEach(function(cf) {
      if (COLUMNS.indexOf(cf.field_name) !== -1 && cf.values && cf.values.length > 0) {
        m[cf.field_name] = cf.values[0].value;
      }
    });
  }

  // Custom fields (form-encoded)
  var cfIdx = {};
  Object.keys(lead).forEach(function(key) {
    var mId   = key.match(/^custom_fields\[(\d+)\]\[id\]$/);
    var mVal  = key.match(/^custom_fields\[(\d+)\]\[values\]\[0\]\[value\]$/);
    var mName = key.match(/^custom_fields\[(\d+)\]\[name\]$/);
    if (mId)   { cfIdx[mId[1]]   = cfIdx[mId[1]]   || {}; cfIdx[mId[1]].id    = lead[key]; }
    if (mVal)  { cfIdx[mVal[1]]  = cfIdx[mVal[1]]  || {}; cfIdx[mVal[1]].value = lead[key]; }
    if (mName) { cfIdx[mName[1]] = cfIdx[mName[1]] || {}; cfIdx[mName[1]].name  = lead[key]; }
  });
  Object.values(cfIdx).forEach(function(cf) {
    if (cf.name && COLUMNS.indexOf(cf.name) !== -1 && cf.value !== undefined) m[cf.name] = cf.value;
  });

  return COLUMNS.map(function(col) { return m[col] !== undefined ? m[col] : ""; });
}

function ensureHeaders(sheet) {
  if (sheet.getLastRow() === 0 || sheet.getRange(1,1).getValue() === "") {
    sheet.getRange(1, 1, 1, COLUMNS.length).setValues([COLUMNS]);
    sheet.getRange(1, 1, 1, COLUMNS.length).setFontWeight("bold");
    sheet.setFrozenRows(1);
  }
}

function findRow(sheet, leadId) {
  if (!leadId) return -1;
  var last = sheet.getLastRow();
  if (last < 2) return -1;
  var ids = sheet.getRange(2, ID_COL_INDEX, last - 1, 1).getValues();
  for (var i = 0; i < ids.length; i++) {
    if (String(ids[i][0]) === String(leadId)) return i + 2;
  }
  return -1;
}

// 
//  amoCRM API HELPERS
// 

function getValidAccessToken() {
  var props = PropertiesService.getScriptProperties();
  var token = props.getProperty("AMO_ACCESS_TOKEN");

  if (token) {
    var test = UrlFetchApp.fetch(
      "https://" + AMO_SUBDOMAIN + ".amocrm.ru/api/v4/account",
      { headers: { Authorization: "Bearer " + token }, muteHttpExceptions: true }
    );
    if (test.getResponseCode() === 200) return token;
  }

  var refresh = props.getProperty("AMO_REFRESH_TOKEN");
  if (refresh) {
    var r = UrlFetchApp.fetch(
      "https://" + AMO_SUBDOMAIN + ".amocrm.ru/oauth2/access_token",
      {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify({
          client_id:     props.getProperty("AMO_CLIENT_ID"),
          client_secret: props.getProperty("AMO_CLIENT_SECRET"),
          grant_type:    "refresh_token",
          refresh_token: refresh,
          redirect_uri:  props.getProperty("AMO_REDIRECT_URI"),
        }),
        muteHttpExceptions: true,
      }
    );
    var result = JSON.parse(r.getContentText());
    if (result.access_token) {
      props.setProperty("AMO_ACCESS_TOKEN",  result.access_token);
      props.setProperty("AMO_REFRESH_TOKEN", result.refresh_token);
      return result.access_token;
    }
  }

  Logger.log("No valid token. Run setupCredentials() then exchangeCode().");
  return null;
}

function amoGet(endpoint) {
  var token = getValidAccessToken();
  if (!token) return null;
  var r = UrlFetchApp.fetch(
    "https://" + AMO_SUBDOMAIN + ".amocrm.ru" + endpoint,
    { headers: { Authorization: "Bearer " + token }, muteHttpExceptions: true }
  );
  return JSON.parse(r.getContentText());
}

function amoPatch(endpoint, body) {
  var token = getValidAccessToken();
  if (!token) return null;
  var r = UrlFetchApp.fetch(
    "https://" + AMO_SUBDOMAIN + ".amocrm.ru" + endpoint,
    {
      method: "patch",
      contentType: "application/json",
      headers: { Authorization: "Bearer " + token },
      payload: JSON.stringify(body),
      muteHttpExceptions: true,
    }
  );
  return JSON.parse(r.getContentText());
}
