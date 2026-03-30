from app.app_manager import AppManager

def main() -> None:
    manager = AppManager()
    manager.initialize()
    manager.start()

if __name__ == "__main__":
    main()
