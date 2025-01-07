import libtorrent as lt
import time
import os

def download_torrent_all(torrent_source, save_path):
    try:
        # Initialize session
        ses = lt.session()

        # Set session options
        settings = {
            'download_rate_limit': 0,  # Unlimited download rate
            'upload_rate_limit': 0,    # Unlimited upload rate
            'connections_limit': 200,  # Increase connection limit
            'active_downloads': 10,    # Allow multiple active downloads
        }
        ses.apply_settings(settings)

        if torrent_source.startswith("magnet:?xt="):
            # Handle magnet link
            params = {
                'save_path': save_path,
                'storage_mode': lt.storage_mode_t.storage_mode_sparse,
            }
            h = lt.add_magnet_uri(ses, torrent_source, params)
        else:
            # Handle .torrent file
            info = lt.torrent_info(torrent_source)
            h = ses.add_torrent({'ti': info, 'save_path': save_path})

        print("Fetching metadata...")
        start_time = time.time()
        while not h.has_metadata():
            if time.time() - start_time > 60:  # Timeout after 60 seconds
                print("Timeout reached, no metadata fetched.")
                break
            time.sleep(1)

        if not h.has_metadata():
            print("Unable to fetch metadata, exiting.")
            return

        # Automatically download all files
        for i in range(h.get_torrent_info().num_files()):
            h.file_priority(i, 1)

        print("Starting torrent download...")
        while not h.is_seed():
            s = h.status()
            print(
                f"\rProgress: {s.progress * 100:.2f}% | "
                f"Peers: {s.num_peers} | "
                f"Download Rate: {s.download_rate / 1000:.2f} kB/s",
                end=''
            )
            alerts = ses.pop_alerts()
            for alert in alerts:
                if isinstance(alert, lt.piece_finished_alert):
                    print(f"\nPiece {alert.piece_index} finished.")
            time.sleep(1)

        print("\nDownload complete.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    torrent_source = input("Enter the torrent file path or magnet link: ")

    if torrent_source.startswith("magnet:?xt=") or os.path.isfile(torrent_source):
        save_path = input("Enter the path to the save directory: ")

        if not os.path.isdir(save_path):
            print("The save directory does not exist.")
        else:
            try:
                print("Magnet link detected. Downloading all files...")
                download_torrent_all(torrent_source, save_path)
            except Exception as e:
                print(f"An error occurred: {e}")
    else:
        print("The provided torrent source is neither a valid magnet link nor a torrent file.")

