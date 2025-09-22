function userGeminiReport() {
  const ORG_UNIT_PATH = "/[OU]";  // name of OU
  const SHEET_ID = "[SHEET ID]";  // sheet to write to
  const HOUR_TRIGGER = 3;
  const START_INDEX = 0;
  const END_INDEX = 4999;

  const CONTACT_FOLDER_ID = "[FOLDER ID]"; // folder where contact info is stored
  const FILENAME = `${ORG_UNIT_PATH.substring(1)}_emails.json`;
  const WEBHOOK_URL = "[WEBHOOK URL]" // Webhook URL to send failures to
  const scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.setProperty("SHEET_ID", SHEET_ID);
  scriptProperties.setProperty("HOUR_TRIGGER", HOUR_TRIGGER);
  scriptProperties.setProperty("WEBHOOK_URL", WEBHOOK_URL);
  const folder = DriveApp.getFolderById(CONTACT_FOLDER_ID);
  let allUserEmails = [];
  let usersToProcess = [];
  try {
    const files = folder.getFilesByName(FILENAME);
    // Check if the file exists in the folder.
    if (files.hasNext()) {
      const file = files.next();
      const jsonContent = file.getBlob().getDataAsString();
      allUserEmails = JSON.parse(jsonContent);
      Logger.log(`Successfully read ${allUserEmails.length} emails from ${FILENAME}.`);
      users = allUserEmails.slice(START_INDEX, END_INDEX);
      scriptProperties.setProperty('USERS', JSON.stringify(users));
    } else {
      Logger.log(`Error: File "${FILENAME}" not found in the specified folder.`);
      return null;
    }
  } catch (e) {
    // Log any other errors that occur during the process.
    Logger.log(`An error occurred while reading the JSON file: ${e.message}`);
    Logger.log(`Stack Trace: ${e.stack}`);
    return null;
  }
  const staffCount = users.length;
  scriptProperties.setProperty("staffCount", staffCount.toString());
  scriptProperties.setProperty("staffProcessed", "0");
  const spreadsheet = SpreadsheetApp.openById(SHEET_ID);
  const sheet = spreadsheet.getSheets()[0];
  sheet.clear();
  sheet.getRange(1, 1, 1, 4).setValues([["User", "App", "Action", "Count"]]);
  SpreadsheetApp.flush();
  _deleteTriggersForThisScript();
  processUserBatch();
}

function processUserBatch() {
  const scriptProperties = PropertiesService.getScriptProperties();
  const SHEET_ID = scriptProperties.getProperty("SHEET_ID");
  const HOUR_TRIGGER = parseInt(scriptProperties.getProperty("HOUR_TRIGGER"), 10);
  const USERS = scriptProperties.getProperty("USERS");
  users = JSON.parse(USERS);
  const staffCount = parseInt(scriptProperties.getProperty("staffCount") || "0", 10);
  let staffProcessed = parseInt(scriptProperties.getProperty("staffProcessed") || "0", 10);
  if (staffProcessed >= staffCount) {
    Logger.log("All users have been processed. Process complete.");
    _deleteTriggersForThisScript(); // Final cleanup of triggers
    return;
  }
  try {
    const startIndex = staffProcessed;
    const endIndex = Math.min(startIndex + 200, staffCount);
    const usersToProcess = users.slice(startIndex, endIndex);
    Logger.log(`Processing users from index ${startIndex} to ${endIndex - 1}. Batch size: ${usersToProcess.length}.`);
    const twentyEightDaysAgo = new Date(new Date().getTime() - 28 * 24 * 60 * 60 * 1000);
    const startTime = twentyEightDaysAgo.toISOString();
    const pivotData = {};
    usersToProcess.forEach((userKey, index) => {
      if (!userKey) return;
      const overallIndex = startIndex + index;
      const batchIndex = index + 1;
      const currentBatchSize = usersToProcess.length;
      const userFetchStartTime = new Date().getTime();
      Logger.log(`Fetching report for user: ${userKey} (${overallIndex}, ${batchIndex} of ${currentBatchSize} in this batch)`);
      let pageToken;
      do {
        let response;
        const maxRetries = 5;
        for (let i = 0; i < maxRetries; i++) {
          try {
            response = AdminReports.Activities.list(
              userKey,
              "gemini_in_workspace_apps",
              {
                eventName: "feature_utilization",
                startTime: startTime,
                filters: "event_category<>inactive,app_name<>workflows",
                pageToken: pageToken,
              }
            );
            break; // Success, exit retry loop
          } catch (e) {
            Logger.log(`Attempt ${i + 1} failed for user ${userKey} in AdminReports.Activities.list: ${e.message}`);
            if (i < maxRetries - 1) {
              const sleepTime =
                Math.pow(2, i + 1) * 1000 + Math.floor(Math.random() * 1000);
              Logger.log(`Retrying in ${sleepTime / 1000} seconds...`);
              Utilities.sleep(sleepTime);
            } else {
              throw e;
            }
          }
        }

        const activities = response.items;
        if (activities && activities.length > 0) {
          activities.forEach((activity) => {
            activity.events.forEach((event) => {
              if (event.name === "feature_utilization") {
                const params = event.parameters.reduce((obj, param) => {
                  obj[param.name] = param.value;
                  return obj;
                }, {});
                if (params["event_category"] === "unknown") return;

                const user = activity.actor.email;
                const app = params["app_name"] || "";
                const action = params["action"] || "";
                const key = `${user}|${app}|${action}`;

                if (pivotData[key]) {
                  pivotData[key].Count++;
                } else {
                  pivotData[key] = {
                    User: user,
                    App: app,
                    Action: action,
                    Count: 1,
                  };
                }
              }
            });
          });
        }
        pageToken = response.nextPageToken;
      } while (pageToken);
      const userFetchEndTime = new Date().getTime();
      const durationSeconds = ((userFetchEndTime - userFetchStartTime) /1000).toFixed(2);
      Logger.log(
        `Fetched report for user: ${userKey} (${overallIndex}, ${batchIndex} of ${currentBatchSize} in this batch) in ${durationSeconds} seconds`
      );
    }); // --- Convert pivotData to array and append to the spreadsheet ---

    const outputData = Object.values(pivotData).map((row) => [
      row.User,
      row.App,
      row.Action,
      row.Count,
    ]);
    if (outputData.length > 0) {
      const sheet = SpreadsheetApp.openById(SHEET_ID).getSheets()[0];
      sheet
        .getRange(sheet.getLastRow() + 1, 1, outputData.length, 4)
        .setValues(outputData);
      Logger.log(
        `Successfully appended ${outputData.length} rows to the spreadsheet.`
      );
    } else {
      Logger.log(`No Gemini usage found for this batch of users.`);
    }

    // --- Update the processed count and schedule the next run ---
    staffProcessed = endIndex;
    scriptProperties.setProperty("staffProcessed", staffProcessed.toString());
    Logger.log(
      `Total staff processed so far: ${staffProcessed} / ${staffCount}`
    );

    if (staffProcessed < staffCount) {
      _deleteTriggersForThisScript();
      ScriptApp.newTrigger("processUserBatch").timeBased().after(1 * 60 * 1000).create();
      Logger.log("Scheduled next batch to run in 1 minute.");
    } else {
      Logger.log("All batches complete. Finalizing process.");
      const scriptName = DriveApp.getFileById(ScriptApp.getScriptId()).getName();
      Logger.log(`${scriptName} finished`);
      _deleteTriggersForThisScript();
      // Trigger to run again at HOUR_TRIGGER
      ScriptApp.newTrigger("userGeminiReport").timeBased().everyDays(1).atHour(HOUR_TRIGGER).create();
    }
  } catch (e) {
    const errorMessage = `Error during batch processing: ${e.toString()}`;
    Logger.log(errorMessage);
    _postErrorToWebhook(errorMessage);
    const sheet = SpreadsheetApp.openById(SHEET_ID).getSheets()[0];
    sheet
      .getRange(sheet.getLastRow() + 1, 1)
      .setValue(`An error occurred during batch processing: ${e.toString()}`);
    _deleteTriggersForThisScript(); // Stop the process on error
  }
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

function _postErrorToWebhook(errorMessage) {
  const scriptProperties = PropertiesService.getScriptProperties();
  const WEBHOOK_URL = scriptProperties.getProperty("WEBHOOK_URL");
  const ORG_UNIT_PATH = scriptProperties.getProperty("ORG_UNIT_PATH");
  const scriptId = ScriptApp.getScriptId();
  const projectUrl = `https://script.google.com/home/projects/${scriptId}/edit`;
  const scriptFile = DriveApp.getFileById(scriptId);
  const scriptName = scriptFile.getName();

  const payload = {
    cardsV2: [
      {
        cardId: "error_card",
        card: {
          header: {
            title: scriptName + " Error",
            subtitle: `${ORG_UNIT_PATH}`,
            imageUrl:
              "https://www.google.com/images/icons/product/apps_script-32.png",
            imageType: "CIRCLE",
          },
          sections: [
            {
              header: "Error Details",
              widgets: [
                {
                  textParagraph: {
                    text: `<b>Message:</b><br>${errorMessage}`,
                  },
                },
              ],
            },
            {
              widgets: [
                {
                  buttonList: {
                    buttons: [
                      {
                        text: "OPEN SCRIPT",
                        onClick: {
                          openLink: {
                            url: projectUrl,
                          },
                        },
                      },
                    ],
                  },
                },
              ],
            },
          ],
        },
      },
    ],
  };
  const options = {
    method: "post",
    contentType: "application/json; charset=UTF-8",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };
  try {
    const response = UrlFetchApp.fetch(WEBHOOK_URL, options);
    Logger.log(
      `Webhook notification sent. Response code: ${response.getResponseCode()}`
    );
  } catch (fetchError) {
    Logger.log(`Failed to post error to webhook: ${fetchError.toString()}`);
  }
}
