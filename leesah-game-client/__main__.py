import main


def run():
    participant, rapid = main.main()
    print("\n\t✅ Started client successfully\n")
    try:
        while rapid.running:
            rapid.run(participant)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown(rapid)


def shutdown(rapid):
    print("\n 🛑 shutting down...")
    rapid.close()


if __name__ == "__main__":
    run()
