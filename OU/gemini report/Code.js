function runFullReportAndAggregation() {
  try {
    // STEP 1: Run the BigQuery report and write the raw data to the target sheet
    runBigQueryReport();

    // STEP 2: Run all aggregation steps and distribution using the data just written
    aggregateData();

    Logger.log('🎉 Full report and aggregation process completed successfully! 🎉');

  } catch (e) {
    Logger.log(`🔥🔥🔥 A critical error occurred during the main process: ${e.message} 🔥🔥🔥`);
    SpreadsheetApp.getUi().alert(`An error occurred: ${e.message}`);
  }
}


// ====================================================================================
// SECTION 1: BIGQUERY DATA FETCH AND INITIAL WRITE
// ====================================================================================

/**
 * Executes a BigQuery query, merges with OU data, sorts, and saves to a specific Google Sheet.
 */
function runBigQueryReport() {
  const properties = PropertiesService.getScriptProperties();
  const projectId = properties.getProperty('BQ_GEMINI_PROJECT_ID');
  const tableName = properties.getProperty('BQ_ACTIVITY_TABLE');
  const targetSpreadsheetId = properties.getProperty('TARGET_SPREADSHEET_ID');
  const rawDataSheetName = "Sheet1";
  const ouSpreadsheetId = properties.getProperty('OU_SPREADSHEET_ID');
  const ouSheetName = "Sheet1";
  const ouLookup = createOULookupMap(ouSpreadsheetId, ouSheetName);

  const query = `
    SELECT
        email AS User,
        gemini_for_workspace.app_name AS App,
        gemini_for_workspace.action AS Action,
        COUNT(*) AS Count
    FROM ${tableName}
    WHERE
        date(_partitiontime) BETWEEN DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 28 DAY) AND CURRENT_DATE("America/New_York")
        AND event_name = "feature_utilization"
        AND gemini_for_workspace.event_category <> "inactive"
        AND gemini_for_workspace.event_category <> "unknown"
    GROUP BY
        User, App, Action`;

  const request = {
    configuration: { query: { query: query, useLegacySql: false } }
  };
  
  Logger.log('Starting the BigQuery query...');
  let queryJob = BigQuery.Jobs.insert(request, projectId);
  const jobId = queryJob.jobReference.jobId;
  let sleepTimeMs = 500;
  while (queryJob.status.state !== 'DONE') {
    Utilities.sleep(sleepTimeMs);
    queryJob = BigQuery.Jobs.get(projectId, jobId);
  }

  if (queryJob.status.errorResult) {
    throw new Error(`BigQuery job failed: ${queryJob.status.errorResult.message}`);
  }
  Logger.log('BigQuery job completed successfully.');

  let queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  if (!queryResults.rows || queryResults.rows.length === 0) {
    Logger.log("Query returned no results. Aborting subsequent steps.");
    throw new Error("BigQuery query returned no results.");
  }

  const bqHeaders = queryResults.schema.fields.map(field => field.name);
  const bqDataRows = queryResults.rows.map(row => row.f.map(cell => cell.v));
  const userIndex = bqHeaders.indexOf("User");

  const finalData = [];
  const finalHeaders = [...bqHeaders, "OU"];
  for (const row of bqDataRows) {
    const userEmail = row[userIndex];
    if (ouLookup.has(userEmail)) {
      const ou = ouLookup.get(userEmail);
      finalData.push([...row, ou]);
    }
  }
  Logger.log(`Merge complete. ${finalData.length} rows matched.`);

  finalData.sort((a, b) => {
    if (a[4] < b[4]) return -1; if (a[4] > b[4]) return 1; // Sort by OU (index 4)
    if (a[0] < b[0]) return -1; if (a[0] > b[0]) return 1; // Sort by User (index 0)
    if (a[1] < b[1]) return -1; if (a[1] > b[1]) return 1; // Sort by App (index 1)
    if (a[2] < b[2]) return -1; if (a[2] > b[2]) return 1; // Sort by Action (index 2)
    return 0;
  });
  Logger.log('Sorting complete.');

  const finalResults = [finalHeaders, ...finalData];
  const spreadsheet = SpreadsheetApp.openById(targetSpreadsheetId);
  let sheet = spreadsheet.getSheetByName(rawDataSheetName);
  if (!sheet) {
      sheet = spreadsheet.insertSheet(rawDataSheetName);
  }
  sheet.clear();
  sheet.getRange(1, 1, finalResults.length, finalResults[0].length).setValues(finalResults);
  Logger.log(`Successfully wrote ${finalData.length} rows to sheet '${rawDataSheetName}'.`);
}


/**
 * Helper to read an OU sheet and create a lookup map.
 * @param {string} spreadsheetId The ID of the Google Sheet containing OU data.
 * @param {string} sheetName The name of the sheet (tab) containing the data.
 * @returns {Map<string, string>} A Map where the key is the email and the value is the OU.
 */
function createOULookupMap(spreadsheetId, sheetName) {
  try {
    const ouSheet = SpreadsheetApp.openById(spreadsheetId).getSheetByName(sheetName);
    const ouValues = ouSheet.getDataRange().getValues();
    const headers = ouValues.shift();
    const emailIndex = headers.indexOf("Primary Email");
    const ouIndex = headers.indexOf("OU");
    if (emailIndex === -1 || ouIndex === -1) {
      throw new Error(`Could not find 'Primary Email' or 'OU' columns in sheet: ${sheetName}.`);
    }
    const ouLookup = new Map();
    for (const row of ouValues) {
      if (row[emailIndex] && row[ouIndex]) {
        ouLookup.set(row[emailIndex].toString(), row[ouIndex].toString());
      }
    }
    Logger.log(`Successfully created OU lookup map with ${ouLookup.size} entries.`);
    return ouLookup;
  } catch (e) {
    Logger.log(`Error reading OU sheet: ${e.message}`);
    throw e;
  }
}


// ====================================================================================
// SECTION 2: DATA AGGREGATION, CHARTING, AND DISTRIBUTION
// ====================================================================================

/**
 * Aggregates data from a source sheet into nine summary sheets and distributes reports.
 * @param {string} spreadsheetId The ID of the main spreadsheet.
 * @param {string} sourceSheetName The name of the sheet with the raw data.
 * @param {string} staffCountSpreadsheetId The ID of the spreadsheet with staff counts.
 * @param {string} staffCountSheetName The name of the sheet with staff counts.
 */
function aggregateData() {
  const spreadsheetId = properties.getProperty('TARGET_SPREADSHEET_ID');
  const sourceSheetName = 'Sheet1';
  const staffCountSpreadsheetId = properties.getProperty('STAFF_COUNT_SPREADSHEET_ID');
  const staffCountSheetName = 'Sheet1';

  Logger.log('Starting data aggregation script...');
  const ss = SpreadsheetApp.openById(spreadsheetId);
  const sourceSheet = ss.getSheetByName(sourceSheetName);

  if (!sourceSheet) {
    Logger.log(`Error: Source sheet "${sourceSheetName}" not found. Aborting script.`);
    return;
  }

  // Get all the data from the raw data sheet
  let allData = sourceSheet.getDataRange().getValues();

  // Get the header row to find column indices dynamically
  const headers = allData.shift(); // Removes the header row from the data array
  const userIndex = headers.indexOf('User');
  const appIndex = headers.indexOf('App');
  const actionIndex = headers.indexOf('Action');
  const countIndex = headers.indexOf('Count');
  const ouIndex = headers.indexOf('OU');

  if ([userIndex, appIndex, actionIndex, countIndex, ouIndex].includes(-1)) {
    const missingCols = ['User', 'App', 'Action', 'Count', 'OU'].filter(h => headers.indexOf(h) === -1);
    const errorMsg = `One or more required columns (${missingCols.join(', ')}) are missing in ${sourceSheetName}.`;
    Logger.log(`Error: ${errorMsg} Aborting script.`);
    return;
  }

  const initialDataCount = allData.length;
  allData = allData.filter(row => row[appIndex] && row[actionIndex]);
  Logger.log(`Filtered data from ${initialDataCount} to ${allData.length} rows, removing entries with a blank 'App' or 'Action'.`);


  // =================================================================
  // Pre-calculate Global Metrics (Used in Sheets 7, 8, and 9)
  // =================================================================
  let totalSumAllData = 0;
  const allUsersSet = new Set();

  allData.forEach(row => {
    totalSumAllData += parseInt(row[countIndex], 10) || 0;
    const user = row[userIndex];
    if (user) {
      allUsersSet.add(user);
    }
  });
  Logger.log(`Pre-calculated total sum of all counts: ${totalSumAllData}`);
  Logger.log(`Pre-calculated total unique users: ${allUsersSet.size}`);


  // =================================================================
  // Fetch and Map Staff Count Data
  // =================================================================
  Logger.log('Fetching staff count data...');
  const staffCountMap = {};
  let totalGeminiStaffCount = 0;
  let staffCountSS; // Declare here to make it available for the write-back process
  const ouSheetIdMap = {}; // This will be filled in the try block

  try {
    staffCountSS = SpreadsheetApp.openById(staffCountSpreadsheetId);
    const staffCountSheet = staffCountSS.getSheetByName(staffCountSheetName);
    const staffCountData = staffCountSheet.getDataRange().getValues();
    const staffHeaders = staffCountData.shift();
    const ouIndexStaff = staffHeaders.indexOf('OU');
    const countIndexStaff = staffHeaders.indexOf('Number of active staff emails');
    const geminiUsageIndexStaff = staffHeaders.indexOf('Gemini usage');
    const ouMapSheetIdIndex = staffHeaders.indexOf('Gemini report sheet id'); // Added for write-back

    if (ouIndexStaff === -1 || countIndexStaff === -1 || geminiUsageIndexStaff === -1 || ouMapSheetIdIndex === -1) {
      const missing = [];
      if (ouIndexStaff === -1) missing.push('OU');
      if (countIndexStaff === -1) missing.push('Number of active staff emails');
      if (geminiUsageIndexStaff === -1) missing.push('Gemini usage');
      if (ouMapSheetIdIndex === -1) missing.push('Gemini report sheet id');
      throw new Error(`Could not find required columns (${missing.join(', ')}) in the staff count sheet.`);
    }

    staffCountData.forEach(row => {
      const ou = row[ouIndexStaff];
      const count = row[countIndexStaff];
      const geminiUsage = row[geminiUsageIndexStaff];
      const sheetId = row[ouMapSheetIdIndex]; // Added for write-back

      if (ou) {
        staffCountMap[ou] = parseInt(count, 10) || 0;
        if (sheetId) { // Added for write-back
          ouSheetIdMap[ou] = sheetId;
        }
      }

      if (geminiUsage == 1) {
        totalGeminiStaffCount += parseInt(count, 10) || 0;
      }
    });
    Logger.log(`Successfully mapped staff count data. Total staff with Gemini usage: ${totalGeminiStaffCount}`);
    Logger.log(`Successfully created OU to Sheet ID map for ${Object.keys(ouSheetIdMap).length} OUs.`); // Added
  } catch (e) {
    Logger.log(`CRITICAL ERROR fetching staff count data: ${e.message}. Aborting script.`);
    return;
  }

  // =================================================================
  // AGGREGATION FOR SHEET 2
  // =================================================================
  Logger.log('Starting aggregation for Sheet2...');
  const aggregation2 = {};

  allData.forEach(row => {
    const user = row[userIndex];
    const action = row[actionIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    const key = action;

    if (!action) return;

    if (!aggregation2[key]) {
      aggregation2[key] = {
        action: action,
        sum: 0,
        users: new Set()
      };
    }
    aggregation2[key].sum += count;
    aggregation2[key].users.add(user);
  });

  const results2 = [];
  for (const key in aggregation2) {
    const group = aggregation2[key];
    results2.push([group.action, group.sum, group.users.size]);
  }

  results2.sort((a, b) => a[0].localeCompare(b[0]));

  const header2 = ['Action', 'Sum(Count)', 'Unique(User)'];
  results2.unshift(header2);

  let targetSheet2 = ss.getSheetByName('Sheet2');
  if (targetSheet2) {
    targetSheet2.clear();
  } else {
    targetSheet2 = ss.insertSheet('Sheet2');
  }
  targetSheet2.getRange(1, 1, results2.length, results2[0].length).setValues(results2);
  Logger.log('Successfully written data to Sheet2.');


  // =================================================================
  // AGGREGATION FOR SHEET 3
  // =================================================================
  Logger.log('Starting aggregation for Sheet3...');
  const aggregation3 = {};

  allData.forEach(row => {
    const user = row[userIndex];
    const app = row[appIndex];
    const action = row[actionIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    const key = app + '|||' + action;

    if (!aggregation3[key]) {
      aggregation3[key] = {
        app: app,
        action: action,
        sum: 0,
        users: new Set()
      };
    }
    aggregation3[key].sum += count;
    aggregation3[key].users.add(user);
  });

  const results3 = [];
  for (const key in aggregation3) {
    const group = aggregation3[key];
    results3.push([group.app, group.action, group.sum, group.users.size]);
  }

  results3.sort((a, b) => {
    const appCompare = a[0].localeCompare(b[0]);
    if (appCompare !== 0) return appCompare;
    return a[1].localeCompare(b[1]);
  });

  const header3 = ['App', 'Action', 'Sum(Count)', 'Unique(User)'];
  results3.unshift(header3);

  let targetSheet3 = ss.getSheetByName('Sheet3');
  if (targetSheet3) {
    targetSheet3.clear();
  } else {
    targetSheet3 = ss.insertSheet('Sheet3');
  }
  targetSheet3.getRange(1, 1, results3.length, results3[0].length).setValues(results3);
  Logger.log('Successfully written data to Sheet3.');

  // =================================================================
  // AGGREGATION FOR SHEET 4
  // =================================================================
  Logger.log('Starting aggregation for Sheet4...');
  const aggregation4 = {};

  allData.forEach(row => {
    const user = row[userIndex];
    const app = row[appIndex];
    const action = row[actionIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    const ou = row[ouIndex];
    const key = ou + '|||' + app + '|||' + action;

    if (!ou || !app || !action) return;

    if (!aggregation4[key]) {
      aggregation4[key] = {
        ou: ou,
        app: app,
        action: action,
        sum: 0,
        users: new Set()
      };
    }
    aggregation4[key].sum += count;
    aggregation4[key].users.add(user);
  });

  const results4 = [];
  for (const key in aggregation4) {
    const group = aggregation4[key];
    results4.push([group.ou, group.app, group.action, group.sum, group.users.size]);
  }

  results4.sort((a, b) => {
    const ouCompare = a[0].localeCompare(b[0]);
    if (ouCompare !== 0) return ouCompare;
    const appCompare = a[1].localeCompare(b[1]);
    if (appCompare !== 0) return appCompare;
    return a[2].localeCompare(b[2]);
  });

  const header4 = ['OU', 'App', 'Action', 'Sum(Count)', 'Unique(User)'];
  results4.unshift(header4);

  let targetSheet4 = ss.getSheetByName('Sheet4');
  if (targetSheet4) {
    targetSheet4.clear();
  } else {
    targetSheet4 = ss.insertSheet('Sheet4');
  }
  targetSheet4.getRange(1, 1, results4.length, results4[0].length).setValues(results4);
  Logger.log('Successfully written data to Sheet4.');


  // =================================================================
  // AGGREGATION FOR SHEET 5
  // =================================================================
  Logger.log('Starting aggregation for Sheet5...');
  const allApps = new Set(allData.map(row => row[appIndex]));
  const sortedApps = Array.from(allApps).sort();
  const userAggregation = {};
  allData.forEach(row => {
    const user = row[userIndex];
    const app = row[appIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    const ou = row[ouIndex];
    if (!user) return;
    if (!userAggregation[user]) {
      userAggregation[user] = {
        ou: ou,
        appCounts: {}
      };
    }
    if (!userAggregation[user].appCounts[app]) {
      userAggregation[user].appCounts[app] = 0;
    }
    userAggregation[user].appCounts[app] += count;
  });

  let resultsForSorting = [];
  const allUsersArr = Object.keys(userAggregation);
  allUsersArr.forEach(user => {
    const userData = userAggregation[user];
    let overallSum = 0;
    const row = [user, userData.ou];
    sortedApps.forEach(app => {
      const count = userData.appCounts[app] || 0;
      row.push(count);
      overallSum += count;
    });
    row.push(overallSum);
    resultsForSorting.push(row);
  });

  const overallIndex = sortedApps.length + 2;
  resultsForSorting.sort((a, b) => b[overallIndex] - a[overallIndex]);
  const header5 = ['User', 'OU', ...sortedApps, 'Overall'];
  const results5 = [header5, ...resultsForSorting];

  let targetSheet5 = ss.getSheetByName('Sheet5');
  if (targetSheet5) {
    targetSheet5.clear();
  } else {
    targetSheet5 = ss.insertSheet('Sheet5');
  }
  if (results5.length > 1) {
    const numRows = results5.length;
    const numCols = results5[0].length;
    targetSheet5.getRange(1, 1, numRows, numCols).setValues(results5);

    const appColumnCount = sortedApps.length;
    if (appColumnCount > 0) {
      targetSheet5.getRange(2, 3, numRows - 1, appColumnCount).setNumberFormat('0');
    }

  } else {
    targetSheet5.getRange(1, 1, 1, header5.length).setValues([header5]);
  }
  Logger.log('Successfully written data to Sheet5.');

  // =================================================================
  // AGGREGATION FOR SHEET 6 (By OU and App with Adoption %)
  // =================================================================
  Logger.log('Starting aggregation for Sheet6...');

  const ouTotals = {};
  allData.forEach(row => {
    const ou = row[ouIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    if (ou) {
      if (!ouTotals[ou]) {
        ouTotals[ou] = 0;
      }
      ouTotals[ou] += count;
    }
  });


  const ouAppAggregation = {};

  allData.forEach(row => {
    const ou = row[ouIndex];
    const app = row[appIndex];
    const user = row[userIndex];
    const action = row[actionIndex];
    const count = parseInt(row[countIndex], 10) || 0;

    if (!ou || !app || !user) return;
    if (!ouAppAggregation[ou]) ouAppAggregation[ou] = {};
    if (!ouAppAggregation[ou][app]) ouAppAggregation[ou][app] = {};
    if (!ouAppAggregation[ou][app][user]) ouAppAggregation[ou][app][user] = {
      actionCounts: {}
    };
    if (!ouAppAggregation[ou][app][user].actionCounts[action]) {
      ouAppAggregation[ou][app][user].actionCounts[action] = 0;
    }
    ouAppAggregation[ou][app][user].actionCounts[action] += count;
  });

  const results6 = [];
  for (const ou in ouAppAggregation) {
    const ouTotalSum = ouTotals[ou] || 0;

    for (const app in ouAppAggregation[ou]) {
      const groupData = ouAppAggregation[ou][app];
      const uniqueUserCount = Object.keys(groupData).length;
      const staffCount = staffCountMap[ou] || 0;
      const adoptionPercentage = (staffCount > 0) ? (uniqueUserCount / staffCount) : 0;

      let groupOverallSum = 0;
      let maxUser = '';
      let maxUserTotalCount = -1;

      for (const user in groupData) {
        const userData = groupData[user];
        const userTotalForGroup = Object.values(userData.actionCounts).reduce((sum, current) => sum + current, 0);
        groupOverallSum += userTotalForGroup;
        if (userTotalForGroup > maxUserTotalCount) {
          maxUserTotalCount = userTotalForGroup;
          maxUser = user;
        }
      }

      const appPercentage = (ouTotalSum > 0) ? (groupOverallSum / ouTotalSum) : 0;

      let maxUserAction = '';
      let maxActionCount = -1;
      if (maxUser) {
        const maxUserActions = ouAppAggregation[ou][app][maxUser].actionCounts;
        for (const action in maxUserActions) {
          if (maxUserActions[action] > maxActionCount) {
            maxActionCount = maxUserActions[action];
            maxUserAction = action;
          }
        }
      }

      results6.push([ou, app, groupOverallSum, appPercentage, uniqueUserCount, staffCount, adoptionPercentage, maxUser, maxUserAction]);
    }
  }

  results6.sort((a, b) => {
    const ouCompare = a[0].localeCompare(b[0]);
    if (ouCompare !== 0) return ouCompare;
    return a[1].localeCompare(b[1]);
  });

  const header6 = ['OU', 'App', 'Sum(Count)', 'App Count %', 'Count(User)', 'Number of active staff emails', 'Adoption %', 'Max(User)', 'Max(User) Action'];
  results6.unshift(header6);

  let targetSheet6 = ss.getSheetByName('Sheet6');
  if (targetSheet6) {
    targetSheet6.clear();
  } else {
    targetSheet6 = ss.insertSheet('Sheet6');
  }

  if (results6.length > 1) {
    const numRows = results6.length;
    targetSheet6.getRange(1, 1, numRows, results6[0].length).setValues(results6);

    targetSheet6.getRange(2, 3, numRows - 1, 1).setNumberFormat('0');
    targetSheet6.getRange(2, 5, numRows - 1, 1).setNumberFormat('0');
    targetSheet6.getRange(2, 4, numRows - 1, 1).setNumberFormat('0.00%');
    targetSheet6.getRange(2, 6, numRows - 1, 1).setNumberFormat('0');
    targetSheet6.getRange(2, 7, numRows - 1, 1).setNumberFormat('0.00%');
  } else {
    targetSheet6.getRange(1, 1, 1, header6.length).setValues([header6]);
  }
  Logger.log('Successfully written data to Sheet6.');

  // =================================================================
  // START: CHART ADDITIONS FOR SHEET 6
  // =================================================================
  const charts6 = targetSheet6.getCharts();
  charts6.forEach(chart => targetSheet6.removeChart(chart));
  Logger.log(`Removed ${charts6.length} existing chart(s) from Sheet6.`);

  if (results6.length > 1) {
    const ouDataRanges = {};
    for (let i = 2; i <= results6.length; i++) {
      const ou = targetSheet6.getRange(i, 1).getValue();
      if (!ouDataRanges[ou]) {
        ouDataRanges[ou] = {
          start: i,
          end: i
        };
      } else {
        ouDataRanges[ou].end = i;
      }
    }

    let chartAnchorRow = 2;
    const chartAnchorCol = header6.length + 2;

    for (const ou in ouDataRanges) {
      const rangeInfo = ouDataRanges[ou];
      const numRowsForChart = rangeInfo.end - rangeInfo.start + 1;

      if (numRowsForChart > 0) {
        const appRange = targetSheet6.getRange(rangeInfo.start, 2, numRowsForChart);
        const percentageRange = targetSheet6.getRange(rangeInfo.start, 4, numRowsForChart);
        const pieChart = targetSheet6.newChart()
          .setChartType(Charts.ChartType.PIE)
          .addRange(appRange)
          .addRange(percentageRange)
          .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
          .setOption('title', `App Usage Distribution for ${ou}`)
          .setPosition(chartAnchorRow, chartAnchorCol, 0, 0)
          .build();
        targetSheet6.insertChart(pieChart);
        Logger.log(`Created pie chart for OU: ${ou}`);

        const adoptionRateRange = targetSheet6.getRange(rangeInfo.start, 7, numRowsForChart);
        const columnChart = targetSheet6.newChart()
          .setChartType(Charts.ChartType.COLUMN)
          .addRange(appRange)
          .addRange(adoptionRateRange)
          .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
          .setOption('title', `App Adoption Rate for ${ou}`)
          .setOption('hAxis', {
            title: 'App'
          })
          .setOption('vAxis', {
            title: 'Adoption %',
            format: '#.##%'
          })
          .setPosition(chartAnchorRow, chartAnchorCol + 8, 0, 0) // Position to the right of the pie chart
          .build();
        targetSheet6.insertChart(columnChart);
        Logger.log(`Created column chart for OU: ${ou}`);

        chartAnchorRow += 18;
      }
    }
  } else {
    Logger.log('Skipping chart creation for Sheet6 as there is no data.');
  }
  // =================================================================
  // END: CHART ADDITIONS FOR SHEET 6
  // =================================================================


  // =================================================================
  // AGGREGATION FOR SHEET 7
  // =================================================================
  Logger.log('Starting aggregation for Sheet7...');
  const appUserActionTotals = {};

  allData.forEach(row => {
    const app = row[appIndex];
    const user = row[userIndex];
    const action = row[actionIndex];
    const count = parseInt(row[countIndex], 10) || 0;
    const ou = row[ouIndex];
    if (!app || !user || !action) return;
    if (!appUserActionTotals[app]) {
      appUserActionTotals[app] = {};
    }
    if (!appUserActionTotals[app][user]) {
      appUserActionTotals[app][user] = {
        ou: ou,
        actionCounts: {}
      };
    }
    if (!appUserActionTotals[app][user].actionCounts[action]) {
      appUserActionTotals[app][user].actionCounts[action] = 0;
    }
    appUserActionTotals[app][user].actionCounts[action] += count;
  });

  const results7 = [];
  for (const app in appUserActionTotals) {
    const appData = appUserActionTotals[app];
    const uniqueUserCount = Object.keys(appData).length;
    const adoptionPercentage = (totalGeminiStaffCount > 0) ? (uniqueUserCount / totalGeminiStaffCount) : 0;
    let appOverallSum = 0;
    let maxUser = '';
    let maxUserOU = '';
    let maxUserTotalCount = -1;

    for (const user in appData) {
      const userData = appData[user];
      const userTotalForApp = Object.values(userData.actionCounts).reduce((sum, current) => sum + current, 0);
      appOverallSum += userTotalForApp;
      if (userTotalForApp > maxUserTotalCount) {
        maxUserTotalCount = userTotalForApp;
        maxUser = user;
        maxUserOU = userData.ou;
      }
    }

    let maxUserAction = '';
    let maxActionCount = -1;
    if (maxUser) {
      const maxUserActions = appUserActionTotals[app][maxUser].actionCounts;
      for (const action in maxUserActions) {
        if (maxUserActions[action] > maxActionCount) {
          maxActionCount = maxUserActions[action];
          maxUserAction = action;
        }
      }
    }

    const appPercentage = (totalSumAllData > 0) ? (appOverallSum / totalSumAllData) : 0;

    results7.push([app, appOverallSum, appPercentage, uniqueUserCount, totalGeminiStaffCount, adoptionPercentage, maxUser, maxUserOU, maxUserAction]);
  }

  results7.sort((a, b) => a[0].localeCompare(b[0]));

  const header7 = ['App', 'Sum(Count)', 'App Count %', 'Count(User)', 'Number of active staff emails', 'Adoption %', 'Max(User)', 'Max(User) OU', 'Max(User) Action'];
  results7.unshift(header7);

  let targetSheet7 = ss.getSheetByName('Sheet7');
  if (targetSheet7) {
    targetSheet7.clear();
  } else {
    targetSheet7 = ss.insertSheet('Sheet7');
  }

  if (results7.length > 1) {
    const numDataRows = results7.length - 1;
    targetSheet7.getRange(1, 1, results7.length, results7[0].length).setValues(results7);

    targetSheet7.getRange(2, 3, numDataRows, 1).setNumberFormat('0.00%');
    targetSheet7.getRange(2, 5, numDataRows, 1).setNumberFormat('0');
    targetSheet7.getRange(2, 6, numDataRows, 1).setNumberFormat('0.00%');

  } else {
    targetSheet7.getRange(1, 1, 1, header7.length).setValues([header7]);
  }
  Logger.log('Successfully written data to Sheet7.');

  // =================================================================
  // START: CHART ADDITIONS FOR SHEET 7
  // =================================================================
  const charts7 = targetSheet7.getCharts();
  charts7.forEach(chart => targetSheet7.removeChart(chart));
  Logger.log(`Removed ${charts7.length} existing chart(s) from Sheet7.`);

  if (results7.length > 1) { // Only create charts if there is data
    const numDataRows = results7.length - 1;
    const appRange = targetSheet7.getRange(2, 1, numDataRows);
    const percentageRange = targetSheet7.getRange(2, 3, numDataRows);
    const pieChart = targetSheet7.newChart()
      .setChartType(Charts.ChartType.PIE)
      .addRange(appRange)
      .addRange(percentageRange)
      .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
      .setOption('title', 'App Usage Distribution by Count %')
      .setPosition(2, 11, 0, 0)
      .build();
    targetSheet7.insertChart(pieChart);
    Logger.log('Successfully created and inserted pie chart into Sheet7.');

    const adoptionRange = targetSheet7.getRange(2, 6, numDataRows);
    const columnChart = targetSheet7.newChart()
      .setChartType(Charts.ChartType.COLUMN)
      .addRange(appRange)
      .addRange(adoptionRange)
      .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
      .setOption('title', 'App Adoption Rate')
      .setOption('hAxis', {
        title: 'App'
      })
      .setOption('vAxis', {
        title: 'Adoption %',
        format: '#.##%'
      })
      .setPosition(20, 11, 0, 0)
      .build();
    targetSheet7.insertChart(columnChart);
    Logger.log('Successfully created and inserted column chart into Sheet7.');

  } else {
    Logger.log('Skipping chart creation for Sheet7 as there is no data.');
  }
  // =================================================================
  // END: CHART ADDITIONS FOR SHEET 7
  // =================================================================


  // =================================================================
  // AGGREGATION FOR SHEET 8
  // =================================================================
  Logger.log('Starting aggregation for Sheet8...');
  const ouAggregation = {};

  allData.forEach(row => {
    const ou = row[ouIndex];
    const user = row[userIndex];
    const count = parseInt(row[countIndex], 10) || 0;

    if (!ou || !user) return;

    if (!ouAggregation[ou]) {
      ouAggregation[ou] = {
        sum: 0,
        users: new Set()
      };
    }
    ouAggregation[ou].sum += count;
    ouAggregation[ou].users.add(user);
  });

  const results8 = [];
  for (const ou in ouAggregation) {
    const group = ouAggregation[ou];
    const uniqueUserCount = group.users.size;
    const staffCount = staffCountMap[ou] || 0;
    const ouPercentage = (totalSumAllData > 0) ? (group.sum / totalSumAllData) : 0;
    const adoptionPercentage = (staffCount > 0) ? (uniqueUserCount / staffCount) : 0;
    results8.push([ou, group.sum, ouPercentage, uniqueUserCount, staffCount, adoptionPercentage]);
  }

  results8.sort((a, b) => b[5] - a[5]);

  const header8 = ['OU', 'Sum(Count)', 'OU Count %', 'Count(User)', 'Number of active staff emails', 'Adoption %'];
  results8.unshift(header8);

  let targetSheet8 = ss.getSheetByName('Sheet8');
  if (targetSheet8) {
    targetSheet8.clear();
  } else {
    targetSheet8 = ss.insertSheet('Sheet8');
  }

  if (results8.length > 1) {
    targetSheet8.getRange(1, 1, results8.length, results8[0].length).setValues(results8);
    targetSheet8.getRange(2, 3, results8.length - 1, 1).setNumberFormat('0.00%');
    targetSheet8.getRange(2, 5, results8.length - 1, 1).setNumberFormat('0');
    targetSheet8.getRange(2, 6, results8.length - 1, 1).setNumberFormat('0.00%');
  } else {
    targetSheet8.getRange(1, 1, 1, header8.length).setValues([header8]);
  }
  Logger.log('Successfully written data to Sheet8.');

  // =================================================================
  // START: CHART ADDITION FOR SHEET 8 (WITH DYNAMIC WIDTH)
  // =================================================================
  const charts8 = targetSheet8.getCharts();
  charts8.forEach(chart => targetSheet8.removeChart(chart));
  Logger.log(`Removed ${charts8.length} existing chart(s) from Sheet8.`);

  if (results8.length > 1) {
    const numDataRows = results8.length - 1;
    const ouRange = targetSheet8.getRange(2, 1, numDataRows);
    const staffCountRange = targetSheet8.getRange(2, 5, numDataRows);
    const adoptionRange = targetSheet8.getRange(2, 6, numDataRows);
    const chartWidth = 200 + (numDataRows * 40);
    const numDataColumns = header8.length;
    const chartStartColumn = numDataColumns + 2;
    const maxCols = targetSheet8.getMaxColumns();
    if (chartStartColumn > maxCols) {
      const neededColumns = chartStartColumn - maxCols;
      targetSheet8.insertColumnsAfter(maxCols, neededColumns);
      Logger.log(`Added ${neededColumns} columns to Sheet8 to make space for the chart.`);
    }

    const comboChart = targetSheet8.newChart()
      .setChartType(Charts.ChartType.COMBO)
      .addRange(ouRange)
      .addRange(adoptionRange)
      .addRange(staffCountRange)
      .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
      .setOption('width', chartWidth)
      .setOption('title', 'OU Adoption Rate and Staff Count')
      .setOption('hAxis', {
        title: 'Organizational Unit',
        slantedText: true,
        slantedTextAngle: 30
      })
      .setOption('vAxes', {
        0: {
          title: 'Adoption %',
          format: '#,##0.00%'
        },
        1: {
          title: 'Number of Staff'
        }
      })
      .setOption('series', {
        0: {
          type: 'bars',
          targetAxisIndex: 0
        },
        1: {
          type: 'line',
          targetAxisIndex: 1
        }
      })
      .setPosition(2, chartStartColumn, 0, 0)
      .build();
    targetSheet8.insertChart(comboChart);
    Logger.log(`Successfully created combo chart in Sheet8, starting in column ${chartStartColumn}.`);
  } else {
    Logger.log('Skipping chart creation for Sheet8 as there is no data.');
  }
  // =================================================================
  // END: CHART ADDITION FOR SHEET 8
  // =================================================================

  // =================================================================
  // AGGREGATION FOR SHEET 9
  // =================================================================
  Logger.log('Starting aggregation for Sheet9...');

  const totalUniqueUsers = allUsersSet.size;
  const adoptionPercentage = (totalGeminiStaffCount > 0) ? (totalUniqueUsers / totalGeminiStaffCount) : 0;

  const header9 = ['Group', 'Sum(Count)', 'Count(User)', 'Number of active staff emails', 'Adoption %'];
  const results9 = [header9, ['All', totalSumAllData, totalUniqueUsers, totalGeminiStaffCount, adoptionPercentage]];

  let targetSheet9 = ss.getSheetByName('Sheet9');
  if (targetSheet9) {
    targetSheet9.clear();
  } else {
    targetSheet9 = ss.insertSheet('Sheet9');
  }

  if (results9.length > 1) {
    targetSheet9.getRange(1, 1, results9.length, results9[0].length).setValues(results9);
    targetSheet9.getRange(2, 5, results9.length - 1, 1).setNumberFormat('0.00%');
  } else {
    targetSheet9.getRange(1, 1, 1, header9.length).setValues([header9]);
  }
  Logger.log('Successfully written data to Sheet9.');

  // =================================================================
  // START: WRITE-BACK TO INDIVIDUAL OU SHEETS
  // =================================================================
  Logger.log('Starting write-back process to individual OU sheets...');

  // Note: ouSheetIdMap was already populated in the staff count fetching block
  
  function getOrCreateSheet(spreadsheet, sheetName) {
    let sheet = spreadsheet.getSheetByName(sheetName);
    if (!sheet) {
      sheet = spreadsheet.insertSheet(sheetName);
      Logger.log(`Created new sheet: "${sheetName}" in spreadsheet: "${spreadsheet.getName()}".`);
    }
    return sheet;
  }

  for (const ou in ouSheetIdMap) {
    const sheetId = ouSheetIdMap[ou];
    if (!sheetId) continue;
    Logger.log(`Processing OU: "${ou}" with Sheet ID: "${sheetId}"`);

    try {
      const targetSS = SpreadsheetApp.openById(sheetId);
      Logger.log(`Successfully opened spreadsheet: "${targetSS.getName()}" for OU: "${ou}".`);

      // --- Write raw data to destiantion Sheet1 ---
      const destSheet1 = getOrCreateSheet(targetSS, 'Sheet1');
      const destHeaders1 = ['User', 'App', 'Action', 'Count'];
      // Filter allData for the current OU and map to just the required columns
      const ouSpecificData = allData.filter(row => row[ouIndex] === ou);
      const mappedData1 = ouSpecificData.map(row => [
        row[userIndex],
        row[appIndex],
        row[actionIndex],
        row[countIndex]
      ]);
      const finalData1 = [destHeaders1, ...mappedData1];
      destSheet1.clear();
      destSheet1.getRange(1, 1, finalData1.length, finalData1[0].length).setValues(finalData1);
      if (finalData1.length > 1) {
        Logger.log(`Wrote ${finalData1.length - 1} raw data rows to Sheet1 for OU "${ou}".`);
      } else {
        Logger.log(`No raw data found for OU "${ou}". Only header written to Sheet1.`);
      }     

      // --- Write Sheet4 data to destination Sheet2 ---
      const filteredResults4WithOU = results4.filter((row, index) => index === 0 || row[0] === ou);
      if (filteredResults4WithOU.length > 1) {
        // Remove the 'OU' column (the first column) from the filtered results
        const filteredResults4 = filteredResults4WithOU.map(row => row.slice(1));

        const destSheet2 = getOrCreateSheet(targetSS, 'Sheet2');
        destSheet2.clear();
        destSheet2.getRange(1, 1, filteredResults4.length, filteredResults4[0].length).setValues(filteredResults4);
        Logger.log(`Wrote ${filteredResults4.length - 1} rows of data to Sheet2 for OU "${ou}".`);
      } else {
        Logger.log(`No data from Sheet4 for OU "${ou}". Skipping write to Sheet2.`);
      }

      // --- Write Sheet5 data to destination Sheet3 ---
      const filteredResults5WithOU = results5.filter((row, index) => index === 0 || row[1] === ou);
      if (filteredResults5WithOU.length > 1) {
        // Remove the 'OU' column (the second column)
        const filteredResults5 = filteredResults5WithOU.map(row => {
          const newRow = [...row]; // Create a copy
          newRow.splice(1, 1); // Remove element at index 1
          return newRow;
        });

        const destSheet3 = getOrCreateSheet(targetSS, 'Sheet3');
        destSheet3.clear();
        destSheet3.getRange(1, 1, filteredResults5.length, filteredResults5[0].length).setValues(filteredResults5);
        // The number of app columns is now total columns - 2 ('User', 'Overall')
        const appColumnCount5 = filteredResults5[0].length - 2;
        if (appColumnCount5 > 0) {
          // App columns now start at column 2 because OU is gone
          destSheet3.getRange(2, 2, filteredResults5.length - 1, appColumnCount5).setNumberFormat('0');
        }

        // Also format the 'Overall' column, which is the last column in the array.
        const overallColumnIndex = filteredResults5[0].length;
        destSheet3.getRange(2, overallColumnIndex, filteredResults5.length - 1, 1).setNumberFormat('0');

        Logger.log(`Wrote ${filteredResults5.length - 1} rows of data to Sheet3 for OU "${ou}".`);
      } else {
        Logger.log(`No data from Sheet5 for OU "${ou}". Skipping write to Sheet3.`);
      }

      // --- Write Sheet6 data to destination Sheet4 (with charts) ---
      const filteredResults6WithOU = results6.filter((row, index) => index === 0 || row[0] === ou);
      if (filteredResults6WithOU.length > 1) {
        // Remove the 'OU' column (the first column)
        const filteredResults6 = filteredResults6WithOU.map(row => row.slice(1));
        const header6WithoutOU = header6.slice(1);

        const destSheet4 = getOrCreateSheet(targetSS, 'Sheet4');
        destSheet4.clear();
        const numDataRows6 = filteredResults6.length - 1;
        destSheet4.getRange(1, 1, filteredResults6.length, filteredResults6[0].length).setValues(filteredResults6);
        // Adjust column indices for number formatting after removing OU column
        destSheet4.getRange(2, 2, numDataRows6, 1).setNumberFormat('0'); // Sum(Count)
        destSheet4.getRange(2, 4, numDataRows6, 1).setNumberFormat('0'); // Count(User)
        destSheet4.getRange(2, 3, numDataRows6, 1).setNumberFormat('0.00%'); // App Count %
        destSheet4.getRange(2, 5, numDataRows6, 1).setNumberFormat('0'); // Number of active staff emails
        destSheet4.getRange(2, 6, numDataRows6, 1).setNumberFormat('0.00%'); // Adoption %
        Logger.log(`Wrote ${numDataRows6} rows of data to Sheet4 for OU "${ou}".`);

        const charts = destSheet4.getCharts();
        charts.forEach(chart => destSheet4.removeChart(chart));

        // Adjust column indices for chart ranges
        const appRange = destSheet4.getRange(2, 1, numDataRows6); // Was col 2, now col 1
        const percentageRange = destSheet4.getRange(2, 3, numDataRows6); // Was col 4, now col 3
        const adoptionRateRange = destSheet4.getRange(2, 6, numDataRows6); // Was col 7, now col 6

        const pieChart = destSheet4.newChart()
          .setChartType(Charts.ChartType.PIE)
          .addRange(appRange).addRange(percentageRange)
          .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
          .setOption('title', `App Usage Distribution for ${ou}`)
          .setPosition(2, header6WithoutOU.length + 2, 0, 0).build(); // Use new header length
        destSheet4.insertChart(pieChart);

        const columnChart = destSheet4.newChart()
          .setChartType(Charts.ChartType.COLUMN)
          .addRange(appRange).addRange(adoptionRateRange)
          .setMergeStrategy(Charts.ChartMergeStrategy.MERGE_COLUMNS)
          .setOption('title', `App Adoption Rate for ${ou}`)
          .setOption('hAxis', {
            title: 'App'
          }).setOption('vAxis', {
            title: 'Adoption %',
            format: '#.##%'
          })
          .setPosition(20, header6WithoutOU.length + 2, 0, 0).build(); // Use new header length
        destSheet4.insertChart(columnChart);
        Logger.log(`Created charts in Sheet4 for OU "${ou}".`);
      } else {
        Logger.log(`No data from Sheet6 for OU "${ou}". Skipping write to Sheet4.`);
      }

      // --- Write Sheet8 data to destination Sheet5 ---
      const filteredResults8WithOU = results8.filter((row, index) => index === 0 || row[0] === ou);
      if (filteredResults8WithOU.length > 1) {
        // Remove the 'OU' (index 0) and 'OU Count %' (index 2) columns
        const filteredResults8 = filteredResults8WithOU.map(row => {
          // For header row, filter by name
          if (row[0] === 'OU') {
            return ['Sum(Count)', 'Count(User)', 'Number of active staff emails', 'Adoption %'];
          }
          // For data rows, filter by index
          return [row[1], row[3], row[4], row[5]];
        });

        const destSheet5 = getOrCreateSheet(targetSS, 'Sheet5');
        destSheet5.clear();
        const numDataRows8 = filteredResults8.length - 1;
        destSheet5.getRange(1, 1, filteredResults8.length, filteredResults8[0].length).setValues(filteredResults8);

        // Adjust column indices for number formatting after removing columns
        destSheet5.getRange(2, 2, numDataRows8, 1).setNumberFormat('0'); // Count(User) is now col 2
        destSheet5.getRange(2, 3, numDataRows8, 1).setNumberFormat('0'); // Number of active staff emails is now col 3
        destSheet5.getRange(2, 4, numDataRows8, 1).setNumberFormat('0.00%'); // Adoption % is now col 4
        Logger.log(`Wrote ${numDataRows8} rows of data to Sheet5 for OU "${ou}".`);
      } else {
        Logger.log(`No data from Sheet8 for OU "${ou}". Skipping write to Sheet5.`);
      }

    } catch (e) {
      Logger.log(`ERROR processing OU "${ou}" with Sheet ID "${sheetId}": ${e.message}. Skipping this OU.`);
    }
  }
  Logger.log('Finished write-back process.');
  // =================================================================
  // END: WRITE-BACK TO INDIVIDUAL OU SHEETS
  // =================================================================


  Logger.log('Data aggregation script finished.');
}