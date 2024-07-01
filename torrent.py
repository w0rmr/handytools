import libtorrent as lt
import time
import os

def download_torrent_part(torrent_file, save_path, file_indices):
    try:
        ses = lt.session()
        info = lt.torrent_info(torrent_file)
        h = ses.add_torrent({'ti': info, 'save_path': save_path})

        # Set all file priorities to 0 (i.e., do not download)
        for i in range(info.num_files()):
            h.file_priority(i, 0)

        # Set selected file priorities to 1 (i.e., normal priority)
        for i in file_indices:
            h.file_priority(i, 1)

        print("Starting torrent download...")
        while not h.is_seed():
            s = h.status()
            print(f"\rProgress: {s.progress * 100:.2f}%", end='')
            alerts = ses.pop_alerts()
            for alert in alerts:
                if alert.what() == "piece_finished_alert":
                    print(f"\nPiece {alert.piece_index} finished.")
            time.sleep(1)

        print("\nDownload complete.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    torrent_file = input("Enter the path to the torrent file: ")
    if not os.path.isfile(torrent_file):
        print("The torrent file does not exist.")
    else:
        save_path = input("Enter the path to the save directory: ")
        if not os.path.isdir(save_path):
            print("The save directory does not exist.")
        else:
            # List all files in the torrent
            try:
                info = lt.torrent_info(torrent_file)
                for i, file in enumerate(info.files()):
                    print(f"{i}: {file.path}")

                file_indices = list(map(int, input("Enter the indices of the files to download, separated by spaces: ").split()))

                download_torrent_part(torrent_file, save_path, file_indices)
            except Exception as e:
                print(f"An error occurred: {e}")