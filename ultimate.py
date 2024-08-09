import libtorrent as lt
import time
import os
import gzip
import json
import queue
from concurrent.futures import ThreadPoolExecutor

def download_file(index, torrent_file, save_path, q):
    try:
        ses = lt.session()
        info = lt.torrent_info(torrent_file)

        h = ses.add_torrent({'ti': info, 'save_path': save_path})

        # Set all file priorities to 0 (i.e., do not download)
        for i in range(info.num_files()):
            h.file_priority(i, 0)

        # Set selected file priority to 1 (i.e., normal priority)
        h.file_priority(index, 1)

        with open('logs', 'a') as log_file:
            print(f"Starting torrent download for file {index}...", file=log_file)
            while True:
                s = h.status()
                progress = s.progress * 100
                print(f"\rProgress: {progress:.2f}%", end='', file=log_file)
                if s.state == lt.torrent_status.seeding or s.progress == 1:
                    break
                time.sleep(1)
            print(f"\nDownload for file {index} complete.", file=log_file)

        # Add the downloaded file to the queue
        file_path = os.path.join(save_path, info.files().at(index).path)
        q.put(file_path)
    except Exception as e:
        with open('logs', 'a') as log_file:
            print(f"An error occurred: {e}", file=log_file)

def process_file(q, country):
    while True:
        file_path = q.get()
        if file_path is None:
            break

        try:
            # Decompress the file
            with gzip.open(file_path, 'rb') as f_in:
                with open(file_path[:-3], 'wb') as f_out:  # remove .gz extension
                    f_out.write(f_in.read())

            # Remove the .gz file
            os.remove(file_path)

            # Process the decompressed file
            with open(file_path[:-3], buffering=200000000) as f:
                # Open the email file in append mode
                with open(f"{country}_emails.txt", 'a') as email_file:
                    for row in f:
                        try:
                            data = json.loads(row)
                        except json.JSONDecodeError as e:
                            with open('logs', 'a') as log_file:
                                print(f"An error occurred: {e}", file=log_file)
                            continue

                        if data and "location_country" in data and data["location_country"] and data["location_country"].lower() == country.lower():
                            for email_entry in data.get("emails", []):
                                email_address = email_entry.get("address")
                                if email_address:
                                    # Write the email directly to the file
                                    email_file.write(email_address + '\n')

            # Remove the decompressed file
            os.remove(file_path[:-3])

        except Exception as e:
            with open('logs', 'a') as log_file:
                print(f"An error occurred: {e}", file=log_file)

        q.task_done()

if __name__ == "__main__":
    torrent_file = "LinkedIn_700M_Data.torrent"  # replace with your torrent file path
    save_path = "."  # replace with your save directory path
    start_index = 1  # replace with your start index
    end_index = 3  # replace with your end index
    country = "spain"  # replace with your country

    if not os.path.isfile(torrent_file):
        print("The torrent file does not exist.")
    elif not os.path.isdir(save_path):
        print("The save directory does not exist.")
    else:
        # Create a queue to communicate between the download and process threads
        q = queue.Queue()

        # Start the download threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            for index in range(start_index, end_index + 1):
                executor.submit(download_file, index, torrent_file, save_path, q)

        # Start the process threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            for _ in range(8):
                executor.submit(process_file, q, country)

        # Block until all tasks are done
        q.join()

        # Stop the process threads
        for _ in range(8):
            q.put(None)