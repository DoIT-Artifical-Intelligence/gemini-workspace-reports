// SHEET ID provided by user
const DESTINATION_SHEET_ID = 'SHEET_ID';
const GEM_MIME_TYPE = 'application/vnd.google-gemini.gem';

// The user to impersonate (Must match the one in getServiceAccount)
// Ideally, keep this variable consistent with the one inside getServiceAccount
const TARGET_OWNER = 'OWNER_EMAIL'; 

/**
 * Main function to run the report
 */
function runGemReport() {
  const service = getServiceAccount();
  
  if (!service.hasAccess()) {
    console.error('Auth Error: ' + service.getLastError());
    return;
  }

  console.log(`Querying Gems for: ${TARGET_OWNER}...`);

  const query = `mimeType = '${GEM_MIME_TYPE}' and '${TARGET_OWNER}' in owners and trashed = false`;
  const fields = "nextPageToken, files(id, name, owners(emailAddress), createdTime, modifiedTime, webViewLink, viewedByMeTime, permissions(role, emailAddress, displayName, domain))";
  
  let pageToken = null;
  let allFiles = [];

  // 2. Pagination Loop (do...while replaces Python while True)
  do {
    const baseUrl = 'https://www.googleapis.com/drive/v3/files';
    
    // Construct URL with parameters. encodieURIComponent is crucial for the query string.
    const params = {
      q: query,
      pageSize: 100,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true, // Deprecated but often required for legacy shared drives
      fields: fields,
      pageToken: pageToken || ''
    };
    
    // Build the query string
    const queryString = Object.keys(params)
      .map(key => key + '=' + encodeURIComponent(params[key]))
      .join('&');

    const url = `${baseUrl}?${queryString}`;

    const response = UrlFetchApp.fetch(url, {
      headers: { Authorization: 'Bearer ' + service.getAccessToken() },
      muteHttpExceptions: true
    });

    const result = JSON.parse(response.getContentText());
    
    if (result.files && result.files.length > 0) {
      allFiles = allFiles.concat(result.files);
    }
    
    pageToken = result.nextPageToken;
    
  } while (pageToken);

  // Header Row
  const rows = [['File Name', 'URL', 'Owner', 'Editor(s)', 'Viewer(s)', 'Created', 'Modified', 'Opened by Me']];

  allFiles.forEach(file => {
    const ownerEmail = (file.owners && file.owners[0]) ? file.owners[0].emailAddress : 'Unknown';
    
    let editors = [];
    let viewers = [];

    if (file.permissions) {
      file.permissions.forEach(perm => {
        // Identity priority: Email -> Display Name -> Domain -> Unknown
        const identity = perm.emailAddress || perm.displayName || perm.domain || "Unknown";

        // Skip if this permission block belongs to the owner (redundant)
        if (identity === ownerEmail) return;

        if (perm.role === 'writer') {
          editors.push(identity);
        } else if (['reader', 'commenter'].includes(perm.role)) {
          viewers.push(identity);
        }
      });
    }

    // Format Dates
    const created = formatDate(file.createdTime);
    const modified = formatDate(file.modifiedTime);
    const openedByMe = formatDate(file.viewedByMeTime);

    rows.push([
      file.name,
      file.webViewLink,
      ownerEmail,
      editors.join(', '),
      viewers.join(', '),
      created,
      modified,
      openedByMe
    ]);
  });

  // 4. Write to Sheet
  if (rows.length > 1) { // If we have data other than headers
    writeToSheet(rows);
  } else {
    console.log("No Gems found.");
  }
}

/**
 * Helper to write 2D array to the Sheet
 */
function writeToSheet(data) {
  const ss = SpreadsheetApp.openById(DESTINATION_SHEET_ID);
  // Selects the first sheet, or you can use .getSheetByName('Sheet1')
  const sheet = ss.getSheets()[0]; 
  
  sheet.clear(); // Clear old data
  
  // setValues requires a rectangular grid, this writes it all at once
  sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
  console.log(`Wrote ${data.length} rows to sheet.`);
}

/**
 * Date Helper (Equivalent to pd.to_datetime + strftime)
 */
function formatDate(isoString) {
  if (!isoString) return "Never";
  // Uses the script's timezone setting
  return Utilities.formatDate(new Date(isoString), Session.getScriptTimeZone(), "MM/dd/yyyy");
}

// --- AUTHENTICATION SECTION (Your provided code) ---

function getServiceAccount() {
  const scriptProperties = PropertiesService.getScriptProperties();
  const privateKey = scriptProperties.getProperty('SA_PRIVATE_KEY');
  const clientEmail = scriptProperties.getProperty('SA_CLIENT_EMAIL');
  
  // Ensure this matches the TARGET_OWNER constant at the top
  const userToImpersonate = 'ray.bell@maryland.gov'; 

  return OAuth2.createService('ServiceAccount')
    .setTokenUrl('https://oauth2.googleapis.com/token')
    .setPrivateKey(privateKey.replace(/\\n/g, '\n'))
    .setIssuer(clientEmail)
    .setSubject(userToImpersonate)
    .setPropertyStore(PropertiesService.getScriptProperties())
    .setScope('https://www.googleapis.com/auth/drive.readonly');
}

function reset() {
  getServiceAccount().reset();
}