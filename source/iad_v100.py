import os
import shutil
import threading
from internetarchive import search_items, get_item, get_files
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_completed_games(log_path):
    if not os.path.exists(log_path):
        return set()
    with open(log_path, 'r') as f:
        return set(line.strip() for line in f.readlines())

def save_completed_game(log_path, game_id, lock):
    with lock:
        with open(log_path, 'a') as f:
            f.write(game_id + '\n')

def download_game(identifier, base_download_dir, completed_log, torrent_only, verbose, remove_unfinished, log_lock):
    # Temporary local download folder (inside current working directory)
    temp_download_dir = os.path.join(os.getcwd(), 'temp_internetarchive_downloads', identifier)
    if not os.path.exists(temp_download_dir):
        os.makedirs(temp_download_dir)
    # Final desired destination folder on external drive
    final_game_folder = os.path.join(base_download_dir, identifier)
    if not os.path.exists(final_game_folder):
        os.makedirs(final_game_folder)
    try:
        item = get_item(identifier)
        
        # Save title and description to a text file inside final_game_folder
        title = item.metadata.get('title', 'No Title')
        description = item.metadata.get('description', 'No Description')
        desc_file_path = os.path.join(final_game_folder, f"{identifier}_description.txt")
        with open(desc_file_path, 'w', encoding='utf-8') as desc_file:
            desc_file.write(f"Title: {title}\n\nDescription:\n{description}\n")
        
        if verbose:
            print(f"Title: {title}")
            print(f"Description: {description}\n")
        
        files = get_files(identifier)
        if not files:
            print(f"{identifier}: skipping, no files found.")
            if remove_unfinished and os.path.exists(temp_download_dir):
                shutil.rmtree(temp_download_dir)
            return False
        if torrent_only:
            files_to_download = [f for f in files if f.name.endswith('.torrent')]
            if not files_to_download:
                print(f"{identifier}: skipping, no torrent file found.")
                if remove_unfinished and os.path.exists(temp_download_dir):
                    shutil.rmtree(temp_download_dir)
                return False
        else:
            files_to_download = files
        # Download files into local temp_download_dir
        for f in files_to_download:
            if verbose:
                print(f"{identifier}: Downloading file {f.name}...")
            f.download(file_path=os.path.join(temp_download_dir, f.name), verbose=verbose)
        # After download completed, move files from temp_download_dir to final_game_folder on external drive
        for root, _, files in os.walk(temp_download_dir):
            for file in files:
                src_file = os.path.join(root, file)
                dst_file = os.path.join(final_game_folder, file)
                shutil.move(src_file, dst_file)
        # Remove temp directory after moving
        shutil.rmtree(temp_download_dir)
        # Log completion
        save_completed_game(completed_log, identifier, log_lock)
        print(f"Downloaded game '{identifier}' successfully.\n")
        return True
    except Exception as e:
        print(f"Error downloading game {identifier}: {e}")
        print("Will retry this game on next run.\n")
        # Cleanup temp directory on failure
        if remove_unfinished and os.path.exists(temp_download_dir):
            shutil.rmtree(temp_download_dir)
        return False

def cleanup_files(base_download_dir, file_types_to_remove, verbose):
    print("Starting cleanup of torrent, image, and metadata files...")
    for item_folder in os.listdir(base_download_dir):
        item_path = os.path.join(base_download_dir, item_folder)
        if not os.path.isdir(item_path):
            continue
        removed_any = False
        for root, _, files in os.walk(item_path):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in file_types_to_remove:
                    try:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        removed_any = True
                        if verbose:
                            print(f"Removed file {file_path}")
                    except Exception as e:
                        print(f"Failed to remove {file_path}: {e}")
        if removed_any and verbose:
            print(f"Finished cleaning some files in {item_path}")
    print("Cleanup completed.")

def download_collection(collection_name, torrent_only, base_download_dir, verbose=False, max_workers=3, remove_unfinished=True):
    completed_log = os.path.join(base_download_dir, 'downloaded_games.log')
    completed_games = load_completed_games(completed_log)
    if not os.path.exists(base_download_dir):
        os.makedirs(base_download_dir)
    print(f"Searching for games in collection '{collection_name}'...")
    games = list(search_items(f'collection:{collection_name}'))
    total_games = len(games)
    print(f"Found {total_games} items in collection '{collection_name}'.")
    log_lock = threading.Lock()
    downloaded_count = 0
    skipped_count = 0
    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, game in enumerate(games, start=1):
            identifier = game['identifier']
            if identifier in completed_games:
                skipped_count += 1
                if verbose:
                    print(f"[{idx}/{total_games}] Skipping already downloaded item: {identifier}")
                continue
            if verbose:
                print(f"[{idx}/{total_games}] Queuing item: {identifier}")
            tasks.append(executor.submit(download_game, identifier, base_download_dir,
                                         completed_log, torrent_only, verbose, remove_unfinished, log_lock))
        for future in as_completed(tasks):
            if future.result():
                downloaded_count += 1
    print(f"Download complete. {downloaded_count} items downloaded, {skipped_count} skipped.")

if __name__ == "__main__":
    print("Internet Archive Collection Downloader")
    collection_name = input("Enter the collection name to download from (e.g. classicpcgames): ").strip()
    torrent_only_input = input("Download torrent files only? (yes/no): ").strip().lower()
    torrent_only = torrent_only_input == 'yes'
    verbose_input = input("Enable verbose output? (yes/no): ").strip().lower()
    verbose = verbose_input == 'yes'
    remove_unfinished_input = input("Remove unfinished downloads on errors? (yes/no): ").strip().lower()
    remove_unfinished = remove_unfinished_input == 'yes'
    base_download_dir = input("Enter the full directory path to save downloads: ").strip()
    max_workers_input = input("Enter number of simultaneous downloads (e.g. 3): ").strip()
    try:
        max_workers = max(1, int(max_workers_input))
    except ValueError:
        max_workers = 3
    download_collection(collection_name, torrent_only, base_download_dir, verbose, max_workers, remove_unfinished)
    cleanup_input = input("Do you want to remove torrent, image, and metadata files from downloaded items? (yes/no): ").strip().lower()
    if cleanup_input == "yes":
        file_types_to_remove = {'.torrent', '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.svg', '.json', '.xml', '.txt'}
        cleanup_files(base_download_dir, file_types_to_remove, verbose)
