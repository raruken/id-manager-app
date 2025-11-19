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
DROPBOX_FILE_PATH = "/test/id_management_file.csv"

st.set_page_config(page_title="ID採番管理", layout="wide")
st.title("📋 ID採番管理")
st.caption("分配PID、分配ID、整備結果IDの年別最終IDを編集できます")

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
        error_info = f"❌ **エラー:** ファイルが見つかりませんでした\n**指定されたパス:** `{path}`"
        return pd.DataFrame(), error_info, None

# ==============================
# CSVの保存
# ==============================
def save_csv_to_dropbox(df, path, original_text=None):
    """DropboxにCSVを保存（元のCSV構造を保持）"""
    try:
        if original_text:
            lines = original_text.split('\n')
            header_cols = lines[0].split(',') if lines else []
            num_cols = len(header_cols) if header_cols else 4
            for i, row in df.iterrows():
                year_str = str(row['年']) if pd.notna(row['年']) and str(row['年']) != 'nan' else ''
                pid_str = str(row['分配PID']) if pd.notna(row['分配PID']) and str(row['分配PID']) != 'nan' else ''
                id_str = str(row['分配ID']) if pd.notna(row['分配ID']) and str(row['分配ID']) != 'nan' else ''
                result_id_str = str(row['整備結果ID']) if pd.notna(row['整備結果ID']) and str(row['整備結果ID']) != 'nan' else ''
                
                if i + 1 < len(lines):
                    values = lines[i + 1].split(',')
                else:
                    values = [''] * num_cols
                    lines.append('')
                
                if len(values) < num_cols:
                    values.extend([''] * (num_cols - len(values)))
                values[0] = year_str
                if num_cols > 1:
                    values[1] = pid_str
                if num_cols > 2:
                    values[2] = id_str
                if num_cols > 3:
                    values[3] = result_id_str
                
                target_index = i + 1 if i + 1 < len(lines) else len(lines) - 1
                lines[target_index] = ','.join(values)
            
            # DataFrameの行数に合わせて行を調整（ヘッダー + データ行）
            expected_lines = len(df) + 1
            if len(lines) > expected_lines:
                lines = lines[:expected_lines]
            csv_content = '\n'.join(lines)
        else:
            # ID列が文字列型であることを確認してからCSVに変換
            csv_df = df.copy()
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
        
        dbx.files_upload(csv_bytes, path, mode=dropbox.files.WriteMode.overwrite)
        return csv_content
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
else:
    st.markdown("---")
    st.subheader("📋 ID管理データ編集")
    st.caption("テーブル下部の「+ Add row」から年度行を追加できます。")
    
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
            num_rows="dynamic",
            column_config={
                "年": st.column_config.TextColumn("年"),
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
    col1, col2 = st.columns([1, 1])
    
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
            try:
                updated_text = save_csv_to_dropbox(
                    df,
                    DROPBOX_FILE_PATH,
                    st.session_state.csv_text_content
                )
                if updated_text is not None:
                    st.session_state.csv_text_content = updated_text
            except Exception as save_error:
                st.error(f"❌ 保存に失敗しました: {save_error}")
                st.stop()
            st.session_state.df = df
            st.success("Dropboxに保存しました")
            st.rerun()