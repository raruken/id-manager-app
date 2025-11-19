import streamlit as st
import pandas as pd
import requests
import dropbox
from io import BytesIO, StringIO
try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False

# ==============================
# Dropbox永続接続処理
# ==============================
def get_dropbox_access_token():
    data = {
        "grant_type": "refresh_token",
        "refresh_token": st.secrets["DROPBOX_REFRESH_TOKEN"],
        "client_id": st.secrets["DROPBOX_APP_KEY"],
        "client_secret": st.secrets["DROPBOX_APP_SECRET"],
    }
    res = requests.post("https://api.dropboxapi.com/oauth2/token", data=data)
    res.raise_for_status()
    return res.json()["access_token"]

ACCESS_TOKEN = get_dropbox_access_token()
dbx = dropbox.Dropbox(ACCESS_TOKEN)

# ==============================
# 設定
# ==============================
DROPBOX_FILE_PATH = "/id_management_file.csv"

st.set_page_config(page_title="ID採番管理", layout="wide")
st.title("📋 ID採番管理")
st.caption("分配PID、分配ID、整備結果IDの年別最終IDを編集できます")

# ==============================
# Dropboxディレクトリ探索
# ==============================
def validate_path(path):
    """パスが有効かどうかを検証"""
    if path is None:
        return False
    if path == "":
        return True
    if not isinstance(path, str):
        return False
    if not path.startswith("/"):
        return False
    return True

def explore_dropbox_path(path):
    """指定されたパスのディレクトリ内容を取得"""
    if not validate_path(path):
        return None
    
    if path == "" or path == "/":
        normalized_path = ""
    else:
        normalized_path = path.rstrip("/")
    
    try:
        result = dbx.files_list_folder(normalized_path)
        return result.entries
    except (dropbox.exceptions.BadInputError, dropbox.exceptions.ApiError):
        return None
    except Exception:
        return None

# ==============================
# CSVの読み込み（Shift-JIS対応）
# ==============================
def load_csv_from_bytes(data, encoding='shift_jis'):
    """バイトデータからCSVを読み込む（Shift-JIS対応）"""
    try:
        try:
            text_data = data.decode(encoding)
        except UnicodeDecodeError:
            try:
                text_data = data.decode('utf-8')
            except UnicodeDecodeError:
                if HAS_CHARDET:
                    try:
                        detected = chardet.detect(data)
                        encoding = detected['encoding'] if detected['encoding'] else 'utf-8'
                        text_data = data.decode(encoding)
                    except:
                        text_data = data.decode('utf-8', errors='ignore')
                else:
                    text_data = data.decode('utf-8', errors='ignore')
        
        # すべての列を文字列として読み込む（IDを文字列として扱うため）
        df = pd.read_csv(StringIO(text_data), header=0, dtype=str, keep_default_na=False)
        
        if len(df.columns) >= 4:
            df_display = pd.DataFrame({
                '年': df.iloc[:, 0].astype(str),
                '分配PID': df.iloc[:, 1].astype(str),
                '分配ID': df.iloc[:, 2].astype(str),
                '整備結果ID': df.iloc[:, 3].astype(str)
            })
        else:
            df_display = df.copy()
            # すべての列を文字列型に統一
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
        
        return df_display, None, text_data
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), "❌ **エラー:** ファイルが空です", None
    except pd.errors.ParserError as pe:
        return pd.DataFrame(), f"❌ **エラー:** CSVファイルの解析に失敗しました\n**詳細:** {pe}", None
    except Exception as e:
        return pd.DataFrame(), f"❌ **エラー:** ファイルの読み込みに失敗しました\n**詳細:** {e}", None

def load_csv_from_dropbox(path):
    """DropboxからCSVを読み込む"""
    try:
        _, res = dbx.files_download(path)
        data = res.content
        df, error_info, text_data = load_csv_from_bytes(data)
        return df, error_info, text_data
    except dropbox.exceptions.ApiError as e:
        error_msg = str(e)
        error_info = ["❌ **エラー:** ファイルが見つかりませんでした"]
        error_info.append(f"**指定されたパス:** `{path}`")
        
        is_not_found = "not_found" in str(e).lower()
        
        if is_not_found:
            path_parts = [p for p in path.split("/") if p]
            if len(path_parts) > 1:
                parent_path = "/" + "/".join(path_parts[:-1])
            else:
                parent_path = ""
            
            if parent_path == "":
                error_info.append(f"\n**親ディレクトリ:** ルートディレクトリ（空文字列）")
            else:
                error_info.append(f"\n**親ディレクトリ:** `{parent_path}`")
            
            try:
                explore_path = parent_path if parent_path != "" else ""
                entries = explore_dropbox_path(explore_path) if explore_path != "" else dbx.files_list_folder("").entries
                if entries:
                    available_files = []
                    available_folders = []
                    for entry in entries:
                        if isinstance(entry, dropbox.files.FileMetadata):
                            available_files.append(f"📄 {entry.name}")
                        elif isinstance(entry, dropbox.files.FolderMetadata):
                            available_folders.append(f"📁 {entry.name}/")
                    
                    if available_files or available_folders:
                        error_info.append("\n**このディレクトリ内のファイル/フォルダ:**")
                        for folder in available_folders:
                            error_info.append(f"  {folder}")
                        for file in available_files:
                            error_info.append(f"  {file}")
            except Exception as explore_error:
                error_info.append(f"\n⚠️ ディレクトリ探索中にエラー: {explore_error}")
        
        return pd.DataFrame(), "\n".join(error_info), None

# ==============================
# CSVの保存
# ==============================
def save_csv_to_dropbox(df, path, original_text=None):
    """DropboxにCSVを保存（元のCSV構造を保持）"""
    try:
        if original_text:
            lines = original_text.split('\n')
            for i, row in df.iterrows():
                if i + 1 < len(lines):
                    values = lines[i + 1].split(',')
                    if len(values) >= 4:
                        # IDを文字列として確実に扱う
                        pid_str = str(row['分配PID']) if pd.notna(row['分配PID']) and str(row['分配PID']) != 'nan' else ''
                        id_str = str(row['分配ID']) if pd.notna(row['分配ID']) and str(row['分配ID']) != 'nan' else ''
                        result_id_str = str(row['整備結果ID']) if pd.notna(row['整備結果ID']) and str(row['整備結果ID']) != 'nan' else ''
                        values[1] = pid_str
                        values[2] = id_str
                        values[3] = result_id_str
                        lines[i + 1] = ','.join(values)
            csv_content = '\n'.join(lines)
            csv_bytes = csv_content.encode('shift_jis')
        else:
            # ID列が文字列型であることを確認してからCSVに変換
            csv_df = df.copy()
            if '分配PID' in csv_df.columns:
                csv_df['分配PID'] = csv_df['分配PID'].astype(str).replace('nan', '')
            if '分配ID' in csv_df.columns:
                csv_df['分配ID'] = csv_df['分配ID'].astype(str).replace('nan', '')
            if '整備結果ID' in csv_df.columns:
                csv_df['整備結果ID'] = csv_df['整備結果ID'].astype(str).replace('nan', '')
            csv_bytes = csv_df.to_csv(index=False).encode("shift_jis")
        
        dbx.files_upload(csv_bytes, path, mode=dropbox.files.WriteMode.overwrite)
        st.success("Dropboxに保存しました。")
    except dropbox.exceptions.ApiError as e:
        st.error(f"Dropboxへの保存エラー: {e}")
        raise

# ==============================
# メイン処理
# ==============================

st.markdown("---")
st.subheader("📁 ファイル読み込み")
uploaded_file = st.file_uploader("id_management_file.csv を選択", type=['csv'], key="csv_uploader")

# セッション状態の初期化
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'csv_text_content' not in st.session_state:
    st.session_state.csv_text_content = None

df = st.session_state.df
error_info = None
csv_text_content = st.session_state.csv_text_content

if uploaded_file is not None:
    file_bytes = uploaded_file.read()
    df, error_info, csv_text_content = load_csv_from_bytes(file_bytes, encoding='shift_jis')
    
    if error_info:
        st.error(error_info)
    elif not df.empty:
        st.success(f"✅ {uploaded_file.name} を読み込みました（Shift-JIS）")
        st.session_state.df = df
        st.session_state.csv_text_content = csv_text_content
else:
    st.info("💡 ローカルファイルをアップロードするか、Dropboxから読み込みます")
    use_dropbox = st.checkbox("Dropboxから読み込む", value=False)
    
    if use_dropbox:
        df, error_info, csv_text_content = load_csv_from_dropbox(DROPBOX_FILE_PATH)
        if error_info:
            st.error(error_info)
        elif not df.empty:
            st.session_state.df = df
            st.session_state.csv_text_content = csv_text_content

if df.empty:
    st.error("❌ ファイルが読み込めませんでした")
    
    if error_info:
        st.markdown(error_info)
    
    st.markdown("---")
    st.subheader("🔍 パス探索機能")
    st.info("以下の機能を使って、正しいファイルパスを見つけてください。")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        explore_path = st.text_input("探索するパスを入力（ルートは空欄または /）", value="", key="explore_path_input", placeholder="空欄でルートディレクトリ、例: /SARTRASサーバー")
    with col2:
        explore_button = st.button("🔍 パスを探索", type="primary", key="explore_button")
    
    if explore_button:
        normalized_explore_path = explore_path.strip()
        if normalized_explore_path == "" or normalized_explore_path == "/":
            normalized_explore_path = ""
            display_path = "ルートディレクトリ（空文字列）"
        else:
            display_path = normalized_explore_path
            if not normalized_explore_path.startswith("/"):
                st.warning("⚠️ パスは `/` で始まる必要があります。")
                normalized_explore_path = None
        
        if normalized_explore_path is not None:
            entries = explore_dropbox_path(normalized_explore_path)
            if entries is not None and len(entries) > 0:
                st.success(f"✅ パス `{display_path}` の内容:")
                
                folders = [e for e in entries if isinstance(e, dropbox.files.FolderMetadata)]
                files = [e for e in entries if isinstance(e, dropbox.files.FileMetadata)]
                
                if folders:
                    st.write("**📁 フォルダ:**")
                    for entry in folders:
                        if normalized_explore_path == "":
                            full_path = f"/{entry.name}"
                        else:
                            full_path = f"{normalized_explore_path.rstrip('/')}/{entry.name}"
                        st.code(full_path, language=None)
                
                if files:
                    st.write("**📄 ファイル:**")
                    for entry in files:
                        if normalized_explore_path == "":
                            full_path = f"/{entry.name}"
                        else:
                            full_path = f"{normalized_explore_path.rstrip('/')}/{entry.name}"
                        file_size_kb = entry.size / 1024
                        st.write(f"`{full_path}` ({file_size_kb:.1f} KB)")
            elif entries is not None and len(entries) == 0:
                st.info(f"ℹ️ パス `{display_path}` は存在しますが、空のディレクトリです。")
            else:
                st.error(f"❌ パス `{display_path}` が見つかりませんでした。")
                if normalized_explore_path != "":
                    st.info("💡 ヒント: ルートディレクトリ（空欄）から探索を始めてください。")
else:
    st.markdown("---")
    st.subheader("📋 ID管理データ編集")
    
    # 年度追加機能
    st.markdown("#### ➕ 年度追加")
    col_add1, col_add2 = st.columns([2, 1])
    with col_add1:
        new_year = st.text_input("追加する年度を入力", key="new_year_input", placeholder="例: 2024")
    with col_add2:
        add_year_button = st.button("📅 年度を追加", type="primary", use_container_width=True, key="add_year_button")
    
    if add_year_button:
        if new_year and new_year.strip():
            new_year_str = str(new_year.strip())
            # 既存の年度を確認
            existing_years = df['年'].astype(str).tolist() if '年' in df.columns else []
            
            if new_year_str in existing_years:
                st.warning(f"⚠️ 年度「{new_year_str}」は既に存在します。")
            else:
                # 新しい年度の行を追加（IDは文字列として初期化）
                new_row = pd.DataFrame({
                    '年': [new_year_str],
                    '分配PID': [''],
                    '分配ID': [''],
                    '整備結果ID': ['']
                }, dtype=str)
                df = pd.concat([df, new_row], ignore_index=True)
                
                # 年度でソート（数値としてソートを試みる）
                try:
                    # 数値としてソート可能か試す
                    df['年_数値'] = df['年'].astype(str).str.extract('(\d+)')[0].astype(float, errors='ignore')
                    df = df.sort_values('年_数値', na_position='last')
                    df = df.drop('年_数値', axis=1)
                except:
                    # 数値としてソートできない場合は文字列としてソート
                    df = df.sort_values('年', na_position='last')
                
                df = df.reset_index(drop=True)
                # ID列を文字列型に確実に変換
                if '分配PID' in df.columns:
                    df['分配PID'] = df['分配PID'].astype(str).replace('nan', '')
                if '分配ID' in df.columns:
                    df['分配ID'] = df['分配ID'].astype(str).replace('nan', '')
                if '整備結果ID' in df.columns:
                    df['整備結果ID'] = df['整備結果ID'].astype(str).replace('nan', '')
                # セッション状態を更新
                st.session_state.df = df
                st.success(f"✅ 年度「{new_year_str}」を追加しました。")
                st.rerun()
        else:
            st.warning("⚠️ 年度を入力してください。")
    
    st.markdown("---")
    
    # デバッグ情報（開発時のみ）
    with st.expander("🔧 デバッグ情報"):
        st.write("**DataFrame型情報:**")
        st.write(df.dtypes)
        st.write("**DataFrame先頭5行:**")
        st.write(df.head())
    
    # 編集可能なテーブル
    try:
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "年": st.column_config.TextColumn("年", disabled=True),
                "分配PID": st.column_config.TextColumn("分配PID"),
                "分配ID": st.column_config.TextColumn("分配ID"),
                "整備結果ID": st.column_config.TextColumn("整備結果ID")
            },
            key="data_editor"
        )
        # ID列を文字列型に確実に変換（データエディタの結果を文字列として保持）
        if '分配PID' in edited_df.columns:
            edited_df['分配PID'] = edited_df['分配PID'].astype(str).replace('nan', '')
        if '分配ID' in edited_df.columns:
            edited_df['分配ID'] = edited_df['分配ID'].astype(str).replace('nan', '')
        if '整備結果ID' in edited_df.columns:
            edited_df['整備結果ID'] = edited_df['整備結果ID'].astype(str).replace('nan', '')
    except Exception as e:
        st.error(f"❌ データエディタエラー: {e}")
        st.info("デバッグ情報を確認して、DataFrame の型を確認してください。")
        st.stop()
    
    st.markdown("---")
    
    # ボタンセクション
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🔄 リセット", use_container_width=True):
            # セッション状態をクリアしてファイルから再読み込み
            if 'df' in st.session_state:
                del st.session_state.df
            if 'csv_text_content' in st.session_state:
                del st.session_state.csv_text_content
            st.rerun()
    
    with col2:
        if st.button("✅ 変更を保存", type="primary", use_container_width=True):
            df = edited_df.copy()
            # ID列を文字列型に確実に変換
            if '分配PID' in df.columns:
                df['分配PID'] = df['分配PID'].astype(str).replace('nan', '')
            if '分配ID' in df.columns:
                df['分配ID'] = df['分配ID'].astype(str).replace('nan', '')
            if '整備結果ID' in df.columns:
                df['整備結果ID'] = df['整備結果ID'].astype(str).replace('nan', '')
            st.session_state.df = df
            st.success("変更を保存しました")
            st.rerun()
    
    with col3:
        if csv_text_content:
            lines = csv_text_content.split('\n')
            for i, row in edited_df.iterrows():
                if i + 1 < len(lines):
                    values = lines[i + 1].split(',')
                    if len(values) >= 4:
                        # IDを文字列として確実に扱う
                        pid_str = str(row['分配PID']) if pd.notna(row['分配PID']) and str(row['分配PID']) != 'nan' else ''
                        id_str = str(row['分配ID']) if pd.notna(row['分配ID']) and str(row['分配ID']) != 'nan' else ''
                        result_id_str = str(row['整備結果ID']) if pd.notna(row['整備結果ID']) and str(row['整備結果ID']) != 'nan' else ''
                        values[1] = pid_str
                        values[2] = id_str
                        values[3] = result_id_str
                        lines[i + 1] = ','.join(values)
            csv_content = '\n'.join(lines)
        else:
            # ID列が文字列型であることを確認してからCSVに変換
            csv_df = edited_df.copy()
            if '分配PID' in csv_df.columns:
                csv_df['分配PID'] = csv_df['分配PID'].astype(str).replace('nan', '')
            if '分配ID' in csv_df.columns:
                csv_df['分配ID'] = csv_df['分配ID'].astype(str).replace('nan', '')
            if '整備結果ID' in csv_df.columns:
                csv_df['整備結果ID'] = csv_df['整備結果ID'].astype(str).replace('nan', '')
            csv_content = csv_df.to_csv(index=False)
        
        try:
            csv_bytes = csv_content.encode('shift_jis')
        except UnicodeEncodeError:
            csv_bytes = ('\uFEFF' + csv_content).encode('utf-8')
        
        st.download_button(
            label="💾 CSVダウンロード",
            data=csv_bytes,
            file_name="id_management_file.csv",
            mime="text/csv",
            use_container_width=True
        )