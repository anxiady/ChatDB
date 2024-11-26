import pandas as pd
from mongo_main import mongo
from mysql_main import sql


if __name__ == "__main__":

    print("\n\n")
    print("\033[92m+\033[0m"*100)
    print("\n\033[92mWelcome to ChatDB!\033[0m \n")

    while True:
        print("\033[92mPlease select a database: \033[0m\n")
        print("1. SQL\n")
        print("2. NoSQL\n")
        print("3. Quit\n")
        choice = input("Enter your choice 1/2/3: ").strip()
        if choice == '1':
            sql()
        elif choice == '2':
            mongo()
        elif choice == '3':
            print("Exiting the program. Goodbye!")
            break
        else:
            raise ValueError("Invalid choice.")

