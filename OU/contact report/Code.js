function userListToSheet() {
  const ORG_UNIT = "[OU]"; // insert OU
  CONTACT_SHEET_ID = "[SHEET ID]"; // insert sheet id to write contacts to

  const TARGET_FOLDER_ID = "[FOLDER ID]"; // insert folder id to write emails to
  const MAX_RETRIES = 5;
  const BATCH_SIZE = 500;
  let allUsers = [];
  let pageToken;

  do {
    let userListResponse;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        userListResponse = AdminDirectory.Users.list({
          customer: "my_customer",
          query: `orgUnitPath='/${ORG_UNIT}' isSuspended=false`,
          projection: "full",
          viewType: "admin_view",
          maxResults: 500,
          pageToken: pageToken,
        });
        break; // Success
      } catch (e) {
        Logger.log(
          `Attempt ${attempt + 1} of ${MAX_RETRIES} failed: ${e.message}`
        );
        if (attempt < MAX_RETRIES - 1) {
          const sleepTime =
            Math.pow(2, attempt + 1) * 1000 + Math.floor(Math.random() * 1000);
          Logger.log(`Retrying in ${sleepTime / 1000} seconds...`);
          Utilities.sleep(sleepTime);
        } else {
          throw new Error(
            `Failed to fetch users after ${MAX_RETRIES} attempts. Last error: ${e.message}`
          );
        }
      }
    }

    if (userListResponse && userListResponse.users) {
      allUsers = allUsers.concat(userListResponse.users);
    }
    pageToken = userListResponse ? userListResponse.nextPageToken : null;
  } while (pageToken);

  if (!allUsers || allUsers.length === 0) {
    Logger.log("No users were found in the specified OU to export.");
    return;
  }
  const headers = [
    "Given Name",
    "Family Name",
    "Manager",
    "Organization",
    "Department",
    "Address",
    "Creation Time",
    "Last Login Time",
    "Service Account",
    "Include In Global Address List",
    "Primary Email",
    "Thumbnail Photo Url",
  ];
  const emails = [];

  const ss = SpreadsheetApp.openById(CONTACT_SHEET_ID);
  const sheet = ss.getSheets()[0];
  sheet.clear();
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]); // Write headers first
  sheet.setFrozenRows(1);

  let dataBatch = [];
  let currentRow = 2; // Data starts on row 2

  allUsers.forEach((user) => {
    const managerRel = user.relations?.find((rel) => rel.type === "manager");
    const organization = user.organizations?.[0]?.title || "";
    const department = user.organizations?.[0]?.department || "";
    const primaryAddress =
      user.addresses?.find((a) => a.primary === true) || user.addresses?.[0];
    const primaryEmail = user.primaryEmail || "";
    const isServiceAccount = user.customSchemas?.Service_Account?.IsService;
    emails.push(primaryEmail); // Still collect all emails for the JSON file

    const row = [
      user.name?.givenName || "",
      user.name?.familyName || "",
      managerRel ? managerRel.value : "",
      organization,
      department,
      primaryAddress ? primaryAddress.formatted : "",
      user.creationTime ? new Date(user.creationTime).toLocaleString() : "",
      user.lastLoginTime
        ? new Date(user.lastLoginTime).toLocaleString()
        : "Never",
      isServiceAccount,
      user.includeInGlobalAddressList,
      primaryEmail,
      user.thumbnailPhotoUrl || "",
    ];
    dataBatch.push(row);

    // If the batch is full, write it to the sheet and reset
    if (dataBatch.length >= BATCH_SIZE) {
      sheet
        .getRange(currentRow, 1, dataBatch.length, headers.length)
        .setValues(dataBatch);
      currentRow += dataBatch.length; // Move the row pointer
      dataBatch = []; // Reset the batch
      SpreadsheetApp.flush(); // Apply the changes immediately
    }
  });

  // Write any remaining rows that didn't fill a full batch
  if (dataBatch.length > 0) {
    sheet
      .getRange(currentRow, 1, dataBatch.length, headers.length)
      .setValues(dataBatch);
  }

  Logger.log(
    `Successfully exported ${allUsers.length} users from ${ORG_UNIT} to the contact sheet https://docs.google.com/spreadsheets/d/${CONTACT_SHEET_ID}.`
  );

  try {
    const folder = DriveApp.getFolderById(TARGET_FOLDER_ID);
    const fileName = `${ORG_UNIT}_emails.json`;
    const existingFiles = folder.getFilesByName(fileName);
    while (existingFiles.hasNext()) {
      const file = existingFiles.next();
      file.setTrashed(true);
      Logger.log(`Moved existing file "${fileName}" to trash.`);
    }
    const jsonContent = JSON.stringify(emails, null, 2);
    folder.createFile(fileName, jsonContent, "application/json");
    Logger.log(`Successfully created "${fileName}" in the target folder.`);
  } catch (e) {
    Logger.log(`Failed to create JSON file: ${e.message}`);
  }
  _deleteTriggersForThisScript();
  ScriptApp.newTrigger("userListToSheet").timeBased().everyDays(1).atHour(18).create();
}

function _deleteTriggersForThisScript() {
  const triggers = ScriptApp.getProjectTriggers();
  if (triggers.length > 0) {
    for (const trigger of triggers) {
      ScriptApp.deleteTrigger(trigger);
    }
    Logger.log(`Cleaned up ${triggers.length} existing script trigger(s).`);
  }
}
