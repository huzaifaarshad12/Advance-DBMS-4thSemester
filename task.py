# import json
# import os

# db_file = "students_db.json"

# def initialize_db():
#     if not os.path.exists(db_file):
#         with open(db_file, 'w') as f:
#             json.dump({"students": []}, f)

# def load_db():
#     with open(db_file, 'r') as f:
#         return json.load(f)

# def save_db(data):
#     with open(db_file, 'w') as f:
#         json.dump(data, f, indent=4)

# def insert_student():
#     db = load_db()
#     student_id = int(input("Enter Student ID: "))
#     if any(s["id"] == student_id for s in db["students"]):
#         print("Error: ID already exists.")
#         return
#     name = input("Enter Name: ")
#     age = int(input("Enter Age: "))
#     course = input("Enter Course: ")
#     gpa = float(input("Enter GPA: "))
    
#     db["students"].append({"id": student_id, "name": name, "age": age, "course": course, "gpa": gpa})
#     save_db(db)
#     print("Student added successfully.")

# def fetch_students():
#     db = load_db()
#     for student in db["students"]:
#         print(student)

# def search_student():
#     db = load_db()
#     search = input("Enter Student ID or Name to search: ")
#     found = [s for s in db["students"] if str(s["id"]) == search or s["name"].lower() == search.lower()]
#     if found:
#         for student in found:
#             print(student)
#     else:
#         print("Student not found.")

# def update_student():
#     db = load_db()
#     student_id = int(input("Enter Student ID to update: "))
#     for student in db["students"]:
#         if student["id"] == student_id:
#             student["name"] = input(f"Enter New Name ({student['name']}): ") or student["name"]
#             student["age"] = int(input(f"Enter New Age ({student['age']}): ") or student["age"])
#             student["course"] = input(f"Enter New Course ({student['course']}): ") or student["course"]
#             student["gpa"] = float(input(f"Enter New GPA ({student['gpa']}): ") or student["gpa"])
#             save_db(db)
#             print("Student updated successfully.")
#             return
#     print("Student ID not found.")

# def delete_student():
#     db = load_db()
#     student_id = int(input("Enter Student ID to delete: "))
#     db["students"] = [s for s in db["students"] if s["id"] != student_id]
#     save_db(db)
#     print("Student deleted successfully.")

# def main():
#     initialize_db()
#     while True:
#         print("\nMenu:")
#         print("1. Insert Student")
#         print("2. View Students")
#         print("3. Search Student")
#         print("4. Update Student")
#         print("5. Delete Student")
#         print("6. Exit")
#         choice = input("Choose an option: ")
        
#         if choice == "1":
#             insert_student()
#         elif choice == "2":
#             fetch_students()
#         elif choice == "3":
#             search_student()
#         elif choice == "4":
#             update_student()
#         elif choice == "5":
#             delete_student()
#         elif choice == "6":
#             print("Exiting...")
#             break
#         else:
#             print("Invalid choice, please try again.")

# if __name__ == "__main__":
#     main()

    
import json
import os

db_file = "D:\Documents\Semester4\Advance DBMS Lab\students_db.json"

def initialize_db():
    """Creates the database file if it doesn't exist."""
    if not os.path.exists(db_file):
        with open(db_file, 'w') as f:
            json.dump({"students": []}, f)

def load_db():
    """Loads the database from JSON file, handling any errors."""
    try:
        with open(db_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print("Database file corrupted or missing. Resetting database.")
        initialize_db()
        return {"students": []}

def save_db(data):
    """Saves the database back to the JSON file."""
    with open(db_file, 'w') as f:
        json.dump(data, f, indent=4)

def insert_student():
    """Inserts a new student record with validation."""
    db = load_db()
    try:
        student_id = int(input("Enter Student ID: "))
        if any(s["id"] == student_id for s in db["students"]):
            print("❌ Error: ID already exists.")
            return
        name = input("Enter Name: ").strip()
        age = int(input("Enter Age: "))
        if age <= 0:
            print("❌ Error: Age must be positive.")
            return
        course = input("Enter Course: ").strip()
        gpa = float(input("Enter GPA: "))
        if not (0.0 <= gpa <= 4.0):
            print("❌ Error: GPA must be between 0.0 and 4.0.")
            return
        
        db["students"].append({"id": student_id, "name": name, "age": age, "course": course, "gpa": gpa})
        save_db(db)
        print("✅ Student added successfully.")
    except ValueError:
        print("❌ Invalid input! Please enter correct data types.")

def fetch_students():
    """Displays all student records."""
    db = load_db()
    if not db["students"]:
        print("📂 No students found in the database.")
        return
    for student in db["students"]:
        print(student)

def search_student():
    """Searches for a student by ID or name."""
    db = load_db()
    search = input("Enter Student ID or Name to search: ").strip()
    found = [s for s in db["students"] if str(s["id"]) == search or s["name"].lower() == search.lower()]
    if found:
        for student in found:
            print(student)
    else:
        print("🔍 Student not found.")

def update_student():
    """Updates an existing student record."""
    db = load_db()
    try:
        student_id = int(input("Enter Student ID to update: "))
        for student in db["students"]:
            if student["id"] == student_id:
                new_name = input(f"Enter New Name ({student['name']}): ").strip()
                new_age = input(f"Enter New Age ({student['age']}): ").strip()
                new_course = input(f"Enter New Course ({student['course']}): ").strip()
                new_gpa = input(f"Enter New GPA ({student['gpa']}): ").strip()

                student["name"] = new_name if new_name else student["name"]
                if new_age:
                    student["age"] = int(new_age)
                    if student["age"] <= 0:
                        print("❌ Error: Age must be positive.")
                        return
                student["course"] = new_course if new_course else student["course"]
                if new_gpa:
                    student["gpa"] = float(new_gpa)
                    if not (0.0 <= student["gpa"] <= 4.0):
                        print("❌ Error: GPA must be between 0.0 and 4.0.")
                        return
                
                save_db(db)
                print("✅ Student updated successfully.")
                return
        print("❌ Student ID not found.")
    except ValueError:
        print("❌ Invalid input! Please enter correct data types.")

def delete_student():
    """Deletes a student record after confirmation."""
    db = load_db()
    try:
        student_id = int(input("Enter Student ID to delete: "))
        student = next((s for s in db["students"] if s["id"] == student_id), None)
        if student:
            confirm = input(f"Are you sure you want to delete {student['name']}? (yes/no): ").strip().lower()
            if confirm == "yes":
                db["students"] = [s for s in db["students"] if s["id"] != student_id]
                save_db(db)
                print("✅ Student deleted successfully.")
            else:
                print("❌ Deletion cancelled.")
        else:
            print("❌ Student ID not found.")
    except ValueError:
        print("❌ Invalid input! Please enter a valid ID.")

def main():
    """Main function to run the menu-driven CLI."""
    initialize_db()
    while True:
        print("\n📌 **Student Database Menu**")
        print("1️⃣ Insert Student")
        print("2️⃣ View Students")
        print("3️⃣ Search Student")
        print("4️⃣ Update Student")
        print("5️⃣ Delete Student")
        print("6️⃣ Exit")
        choice = input("Choose an option: ").strip()
        
        if choice == "1":
            insert_student()
        elif choice == "2":
            fetch_students()
        elif choice == "3":
            search_student()
        elif choice == "4":
            update_student()
        elif choice == "5":
            delete_student()
        elif choice == "6":
            print("🚪 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice, please try again.")

if __name__ == "__main__":
    main()
