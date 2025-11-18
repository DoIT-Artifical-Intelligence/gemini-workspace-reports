def user_shared_gems(owner: str, service_account_file: str = 'service_account_key.json') -> pd.DataFrame:
    """
    Impersonates the 'owner' via Service Account to find all Gem files owned by them.
    Returns a Pandas DataFrame with details including Editors and Viewers.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    GEM_MIME_TYPE = 'application/vnd.google-gemini.gem'
    # 1. Authenticate as the specific user (Domain-Wide Delegation)
    creds = service_account.Credentials.from_service_account_file(
        service_account_file, 
        scopes=SCOPES
    ).with_subject(owner)
    service = build('drive', 'v3', credentials=creds)
    query = f"mimeType = '{GEM_MIME_TYPE}' and '{owner}' in owners and trashed = false"
    fields = "nextPageToken, files(id, name, owners, createdTime, modifiedTime, webViewLink, viewedByMeTime, permissions)"
    all_files = []
    page_token = None
    print(f"Querying Gems for: {owner}...")
    while True:
        results = service.files().list(
            q=query,
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields=fields,
            pageToken=page_token
        ).execute()
        all_files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    data_rows = []
    for file in all_files:
        owner_email = file.get('owners', [{}])[0].get('emailAddress', 'Unknown')
        editors = []
        viewers = []
        if 'permissions' in file:
            for perm in file['permissions']:
                # Identity priority: Email -> Display Name -> Domain -> Unknown
                identity = perm.get('emailAddress') or perm.get('displayName') or perm.get('domain') or "Unknown"
                # Skip if this permission block belongs to the owner (redundant)
                if identity == owner_email:
                    continue
                role = perm.get('role')
                if role == 'writer':
                    editors.append(identity)
                elif role in ['reader', 'commenter']:
                    viewers.append(identity)
        row = {
            "File Name": file.get('name'),
            "URL": file.get('webViewLink'),
            "Owner": owner_email,
            "Editor(s)": ", ".join(editors),
            "Viewer(s)": ", ".join(viewers),
            "Created": file.get('createdTime'),
            "Modified": file.get('modifiedTime'),
            "Opened by Me": file.get('viewedByMeTime', 'Never')
        }
        data_rows.append(row)
    df = pd.DataFrame(data_rows)
    if not df.empty:
        time_cols = ['Created', 'Modified', 'Opened by Me']
        for col in time_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
            df[col] = df[col].fillna("") 
    return df