import subprocess
import sys

def start_server(port):
    """Launches a new terminal and starts the server on a given port."""
    command = [sys.executable, "nodeRAFT.py", str(port)]

    if sys.platform.startswith("win"):  # Windows
        subprocess.Popen(["start", "cmd", "/k"] + command, shell=True)
    elif sys.platform.startswith("linux"):  # Linux
        subprocess.Popen(["gnome-terminal", "--"] + command)
    elif sys.platform.startswith("darwin"):  # macOS
        subprocess.Popen(["osascript", "-e",
            f'tell application "Terminal" to do script "python3 single_server.py {port}"'])
    else:
        print(f"Unsupported OS: {sys.platform}")

if __name__ == "__main__":
    base_port = 50051  # Starting port for servers
    for i in range(5):
        start_server(base_port + i)