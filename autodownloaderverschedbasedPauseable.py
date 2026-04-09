from datetime import datetime, timedelta

import os
import time, uuid, shutil

# outside library
import fitz
import requests
# outside library

from urllib.parse import urlparse

# delete download folder
import shutil


# ACCESS_KEY = os.getenv("B2_ACCESS_KEY_ID")
# SECRET_KEY = os.getenv("B2_SECRET_ACCESS_KEY")
# ENDPOINT = os.getenv("B2_ENDPOINT")
# BUCKET_NAME = os.getenv("B2_BUCKET_NAME")




# ────────────────────────────────
# Main execution for (query, values)
# UNUSED, USE API CALL TO FMS Api_public.php in controller INSTEAD
# ────────────────────────────────
# def retryQuery(query, values, retries=3, delay=2):
#     attempt = 0
#     while attempt < retries:
#         try:
#             with sync_engine.begin() as connection:
#                 result = connection.execute(query, values)
#                 return result  # success
#         except OperationalError as e:  # common for deadlocks/locks
#             attempt += 1
#             print_and_log(f"[Retry {attempt}/{retries}] OperationalError: {e}")
#             if attempt < retries:
#                 time.sleep(delay)
#             else:
#                 raise
#         except SQLAlchemyError as e:  # other SQLAlchemy errors
#             print_and_log(f"Non-retryable SQLAlchemy error: {e}")
#             raise     

# ────────────────────────────────
# Helper
# ────────────────────────────────
def is_valid_url(url):
    if not isinstance(url, str):
        return False
    parsed = urlparse(url)
    return all([parsed.scheme, parsed.netloc])

def sliceUrlLink(url):
    if not url or '/' not in url:
        return ''
    
    last = url.rstrip('/').split('/')[-1]
    return last if last.isdigit() else ''


def getScheduleStatus(url):
    print_and_log("Checking status in main_schedule:", url)
    sched_idx = sliceUrlLink(url)
    if not sched_idx:
        return None
    
    post_url = 'https://fms.jadintracker.id/api/public/get/sched_status'
    try:
        payload = {"sched_idx": sched_idx}
        r = requests.post(
            post_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        if r:
            return r.text
        r.raise_for_status()

        try:
            data = r.json()
            if isinstance(data, dict):
                return int(data.get("sched_status"))
            if isinstance(data, list):
                return int(data[0]["sched_status"])
        except ValueError:
            return int(r.text.strip())
        return None
        
    except Exception as e:
        print_and_log(f"Cannot determine query result, proceeds to default choice... - {e}")
        return None
    

def downloadexcel(no, url, save_folder, max_retries=100, delay_between_retries=10):
    attempt = 0
    safe_title = ""
    while attempt < max_retries:
        final_filename = ""
        safe_title = ""
        try:
            timeout_duration = 60
            response = requests.get(url, stream=True, timeout=timeout_duration)
            if response.status_code == 200:
                # Simpan sementara
                # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                timestamp = str(time.time_ns())

                temp_filename = os.path.join(save_folder, f"temp_{no}_{timestamp}.pdf")
                with open(temp_filename, "wb") as pdf_file:
                    for chunk in response.iter_content(chunk_size=1024):
                        pdf_file.write(chunk)

                # Baca metadata
                with fitz.open(temp_filename) as pdf_doc:
                    metadata = pdf_doc.metadata
                    title = metadata.get("title", "").strip()

                # Gunakan title jika tersedia
                if title:
                    # Hindari karakter ilegal di nama file
                    title = "".join(c if c.isalnum() or c in " ._-" else "_" for c in title)
                    # safe_title = f"{title} - {no}_{timestamp}.pdf"
                    safe_title = f"{title}.pdf"
                else:
                    safe_title = f"{no}_{timestamp}_file.pdf"

                final_filename = os.path.join(save_folder, safe_title)

                # os.rename(temp_filename, final_filename)
                os.replace(temp_filename, final_filename)
                print_and_log(f"No {no} : Berhasil mendownload: {final_filename}")
                return attempt, safe_title, 'success'
            elif response.status_code == 404:
                print_and_log(f"Link {url} status 404, skipping...")
                return attempt, safe_title, 'failed'    
            else:
                attempt += 1
                print_and_log(f"No {no} : Gagal attempt {attempt}/{max_retries} dengan kode : {response.status_code}")
                if attempt < max_retries:
                    print_and_log(f"No {no} : Mencoba ulang dalam {delay_between_retries} detik...")
                    time.sleep(delay_between_retries)
                else:
                    print_and_log(f"No {no} : Gagal setelah {max_retries} percobaan.")
                    return attempt, safe_title, 'failed'    

        except requests.exceptions.RequestException as e:
            attempt += 1
            print_and_log(f"No {no} : Gagal attempt {attempt}/{max_retries} - {e}")
            if attempt < max_retries:
                print_and_log(f"No {no} : Mencoba ulang dalam {delay_between_retries} detik...")
                time.sleep(delay_between_retries)
            else:
                print_and_log(f"No {no} : Gagal setelah {max_retries} percobaan.")
        except Exception as e:
            print_and_log(f"No {no} : Error tidak terduga saat mendownload {url}: {e}")

    return attempt, safe_title, 'failed'


# make it into one file (.zip)
def zip_downloads_folder(save_folder):
    compress_folder = "resources/compress-files"

    current_time = datetime.now() + timedelta(hours=7)
    formatted_time = current_time.strftime("%Y-%m-%d")

    # Generate a UUID
    unique_id = str(uuid.uuid4())
    fileName = f"{formatted_time}-JDN-{unique_id}.zip"
    # Create the zip filename with the UUID
    zip_filename = os.path.join(compress_folder, fileName)


    shutil.make_archive(zip_filename.replace(".zip", ""), "zip", save_folder)
    print_and_log(f"All files zipped successfully : {zip_filename}")

    # Remove all files & subdirectories from downloads folder
    for file_or_folder in os.listdir(save_folder):
        file_path = os.path.join(save_folder, file_or_folder)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)  # Remove file
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove subdirectory
        except Exception as e:
            print_and_log(f"Error deleting {file_path}: {e}")


    shutil.rmtree(save_folder)
    print_and_log(f"Emptied folder: {save_folder}")

    return fileName

def move_files_done_pending(section, filename, root_path):
    try:
        src = os.path.join(root_path, filename)
        
        dst_dir = os.path.join(root_path, section)
        os.makedirs(dst_dir, exist_ok=True)
        
        dst = os.path.join(root_path, section, filename)
        shutil.move(src, dst)
        
        print_and_log(f"Moving {src} to {dst}")
    except Exception as e:
        print_and_log(f"Failed to move PENDING file: {filename} into '{root_path}/{section}', {e}")

def collect_query_sched(table_name):
    try:
        post_url = 'https://fms.jadintracker.id/api/public/post/backup_query'
        
        payload = {"table_name": table_name}
        r = requests.post(post_url, json=payload)

        if r.status_code != 200:
            # r.raise_for_status()
            print_and_log(f"HTTP error | status={r.status_code} | response={r.text}")
            return None

        try:
            data = r.json()
            # print_and_log(data)

            if isinstance(data, dict):
                return data.get("sched_idx")

            if isinstance(data, list):
                return [item.get("sched_idx") for item in data]

        except ValueError:
            return r.text.strip()

        return None

    except Exception as e:
        print_and_log(f"Error: {e}")
        return None
    
    
def update_query_sched(table_name, status_download, sched_idx):
    try:
        post_url = 'https://fms.jadintracker.id/api/public/post/update_backup_query'
        
        payload = {
            "table_name": table_name, 
            "status_download": status_download, 
            "sched_idx": sched_idx
            }
        r = requests.post(
            post_url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        if r.status_code != 200:
            # r.raise_for_status()
            print_and_log(f"HTTP error | status={r.status_code} | response={r.text}")
            return False
        
        return True
    except Exception as e:
        print_and_log(f"Exception triggered: {e}")
        return False
    

def upload_file_sched(result_download, save_folder, table_name, sched_idx):
    try:
        post_url = "https://fms.jadintracker.id/api/public/post/upload_sched_pdf"
        file_path = os.path.join(save_folder, result_download)

        if not os.path.exists(file_path):
            raise Exception("file not found")

        with open(file_path, 'rb') as f:
            files = {'file': (result_download, f)}
            data = {'filename': result_download, 'table_name': table_name, 'sched_idx': sched_idx}

            r = requests.post(post_url, files=files, data=data)

        if r.status_code == 200:
            try:
                res = r.json()
                if res.get("status") == "success":
                    os.remove(file_path)
                    print_and_log(f"Uploaded & deleted: {result_download}")
                    return True
                else:
                    print_and_log(f"Uploaded but failed to delete: {e}")
                    return False
            except:
                print_and_log(f"Invalid JSON | status={r.status_code} | response={r.text}")
                return False
        else:
            print_and_log(f"HTTP error | status={r.status_code} | response={r.text}")
            return False
        

    except Exception as e:
        print_and_log(f"Upload error: {e}")
        return False
    

#chimichanga
START_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"process_{START_TIME}.log"

def print_and_log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{now}] {msg}"
    
    print(full_msg)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")
        
def main():
    if 1 == 1:
        try:
            # table_name = input("Target table_name with sched_idx: ")
            table_name = 'backup_main_schedule_2021_full'

            save_folder = f"resources/downloads/upload_back"
            os.makedirs(save_folder, exist_ok=True)

            if 'bread' != 'key':
                list_sched_idx = collect_query_sched(table_name)
                
                print_and_log(list_sched_idx)
                if list_sched_idx:
                    main_num = 1
                    
                    for sched_idx in list_sched_idx:
                        print_and_log(f"Downloading sched_idx: {sched_idx}")
                        
                        url = f'https://fms.jadintracker.id/print/prints/download_pdf_selesai/{sched_idx}'
                        try:
                            if is_valid_url(url):
                                # Mark sched as pending
                                update_res = update_query_sched(table_name, 'pending', sched_idx)
                                if not update_res:
                                    print_and_log(f"Failed to update status of: {result_download}")
                                    return
                                
                                print_and_log(f"Attempting to download this link: {url}")
                                attempt, result_download, status_success = downloadexcel(f"{main_num}", url, save_folder)
                                
                                if result_download:
                                    if status_success == 'success':
                                        # Upload back to server
                                        if upload_file_sched(result_download, save_folder, table_name, sched_idx):
                                            # Mark sched as done
                                            update_res = update_query_sched(table_name, 'done', sched_idx)
                                            if not update_res:
                                                print_and_log(f"Failed to update status of: {result_download}")
                                                return
                                            
                                            print_and_log(f"Successfully uploaded {result_download}")
                                        else:
                                            print_and_log(f"Upload failed for {sched_idx}")
                                            return
                                    else:
                                        raise Exception("no status_success")
                                else:
                                    raise Exception("false result_download")
                            else:
                                raise Exception("not valid url")
                            
                            main_num += 1
                        except Exception as e:
                            print_and_log(f"Failed URL: {url} | Reason: {e}")
        except Exception as e:
            print_and_log(f"Error : {e}")


# compiler to test a feature
if __name__ == '__main__':
    # params = {'feature': 'autodownloader', 'payload': {'path': '/uploads/1752229544_job08_test_dasd.xlsx', 'filename': '1752229544_job08_test_dasd.xlsx', 'sheet': 'Sheet1', 'taskId': 168, 'source_column': 'LINK_WOD.1', 'target_column': 'NAME_FILE', 'multiple_links': 1, 'delimiter': '|'}, 'RECON_SERVER': 'http://localhost:3000'}
    main()