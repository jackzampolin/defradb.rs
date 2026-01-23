package schemas

import "fmt"

// RelationsSchema tests various relationship patterns:
// - One-to-One (Person <-> Profile)
// - One-to-Many (Author -> Books)
// - Many-to-Many via junction (Student <-> Course via Enrollment)
// - Self-referential (Employee -> Manager)
const RelationsSchema = `
type Person {
	name: String
	email: String
	profile: Profile @relation
}

type Profile {
	bio: String
	avatar: String
	website: String
	person: Person @relation @primary
}

type Author {
	name: String
	country: String
	books: [Book] @relation
}

type Book {
	title: String
	isbn: String
	publishYear: Int
	genre: String
	author: Author @relation
}

type Student {
	studentId: String
	name: String
	major: String
	enrollments: [Enrollment] @relation
}

type Course {
	courseCode: String
	title: String
	credits: Int
	department: String
	enrollments: [Enrollment] @relation
}

type Enrollment {
	enrollmentDate: DateTime
	grade: String
	status: String
	student: Student @relation
	course: Course @relation
}

type Employee {
	employeeId: String
	name: String
	title: String
	department: String
	hireDate: DateTime
	manager: Employee @relation
	directReports: [Employee] @relation
}
`

// RelationsCollectionNames contains collection names for relations schema.
var RelationsCollectionNames = struct {
	Person     string
	Profile    string
	Author     string
	Book       string
	Student    string
	Course     string
	Enrollment string
	Employee   string
}{
	Person:     "Person",
	Profile:    "Profile",
	Author:     "Author",
	Book:       "Book",
	Student:    "Student",
	Course:     "Course",
	Enrollment: "Enrollment",
	Employee:   "Employee",
}

// PersonDoc represents input data for creating a Person document.
type PersonDoc struct {
	Name  string
	Email *string
}

// ProfileDoc represents input data for creating a Profile document.
type ProfileDoc struct {
	Bio      *string
	Avatar   *string
	Website  *string
	PersonID string // Document ID of the related Person
}

// AuthorDoc represents input data for creating an Author document.
type AuthorDoc struct {
	Name    string
	Country *string
}

// BookDoc represents input data for creating a Book document.
type BookDoc struct {
	Title       string
	ISBN        *string
	PublishYear *int
	Genre       *string
	AuthorID    string // Document ID of the related Author
}

// StudentDoc represents input data for creating a Student document.
type StudentDoc struct {
	StudentID string
	Name      string
	Major     *string
}

// CourseDoc represents input data for creating a Course document.
type CourseDoc struct {
	CourseCode string
	Title      string
	Credits    *int
	Department *string
}

// EnrollmentDoc represents input data for creating an Enrollment document.
type EnrollmentDoc struct {
	EnrollmentDate *string
	Grade          *string
	Status         *string
	StudentID      string // Document ID of the related Student
	CourseID       string // Document ID of the related Course
}

// EmployeeDoc represents input data for creating an Employee document.
type EmployeeDoc struct {
	EmployeeID string
	Name       string
	Title      *string
	Department *string
	HireDate   *string
	ManagerID  *string // Document ID of the related Manager (optional, nullable)
}

// CreatePerson generates a GraphQL mutation to create a Person document.
func CreatePerson(doc PersonDoc) string {
	return fmt.Sprintf(`mutation {
		create_Person(input: {
			name: %q
			email: %s
		}) {
			_docID
			name
		}
	}`,
		doc.Name,
		nullableString(doc.Email),
	)
}

// CreateProfile generates a GraphQL mutation to create a Profile document.
func CreateProfile(doc ProfileDoc) string {
	return fmt.Sprintf(`mutation {
		create_Profile(input: {
			bio: %s
			avatar: %s
			website: %s
			person_id: %q
		}) {
			_docID
		}
	}`,
		nullableString(doc.Bio),
		nullableString(doc.Avatar),
		nullableString(doc.Website),
		doc.PersonID,
	)
}

// CreateAuthor generates a GraphQL mutation to create an Author document.
func CreateAuthor(doc AuthorDoc) string {
	return fmt.Sprintf(`mutation {
		create_Author(input: {
			name: %q
			country: %s
		}) {
			_docID
			name
		}
	}`,
		doc.Name,
		nullableString(doc.Country),
	)
}

// CreateBook generates a GraphQL mutation to create a Book document.
func CreateBook(doc BookDoc) string {
	return fmt.Sprintf(`mutation {
		create_Book(input: {
			title: %q
			isbn: %s
			publishYear: %s
			genre: %s
			author_id: %q
		}) {
			_docID
			title
		}
	}`,
		doc.Title,
		nullableString(doc.ISBN),
		nullableInt(doc.PublishYear),
		nullableString(doc.Genre),
		doc.AuthorID,
	)
}

// CreateStudent generates a GraphQL mutation to create a Student document.
func CreateStudent(doc StudentDoc) string {
	return fmt.Sprintf(`mutation {
		create_Student(input: {
			studentId: %q
			name: %q
			major: %s
		}) {
			_docID
			studentId
			name
		}
	}`,
		doc.StudentID,
		doc.Name,
		nullableString(doc.Major),
	)
}

// CreateCourse generates a GraphQL mutation to create a Course document.
func CreateCourse(doc CourseDoc) string {
	return fmt.Sprintf(`mutation {
		create_Course(input: {
			courseCode: %q
			title: %q
			credits: %s
			department: %s
		}) {
			_docID
			courseCode
			title
		}
	}`,
		doc.CourseCode,
		doc.Title,
		nullableInt(doc.Credits),
		nullableString(doc.Department),
	)
}

// CreateEnrollment generates a GraphQL mutation to create an Enrollment document.
func CreateEnrollment(doc EnrollmentDoc) string {
	return fmt.Sprintf(`mutation {
		create_Enrollment(input: {
			enrollmentDate: %s
			grade: %s
			status: %s
			student_id: %q
			course_id: %q
		}) {
			_docID
		}
	}`,
		nullableString(doc.EnrollmentDate),
		nullableString(doc.Grade),
		nullableString(doc.Status),
		doc.StudentID,
		doc.CourseID,
	)
}

// CreateEmployee generates a GraphQL mutation to create an Employee document.
func CreateEmployee(doc EmployeeDoc) string {
	managerClause := ""
	if doc.ManagerID != nil {
		managerClause = fmt.Sprintf(`manager_id: %q`, *doc.ManagerID)
	}
	return fmt.Sprintf(`mutation {
		create_Employee(input: {
			employeeId: %q
			name: %q
			title: %s
			department: %s
			hireDate: %s
			%s
		}) {
			_docID
			employeeId
			name
		}
	}`,
		doc.EmployeeID,
		doc.Name,
		nullableString(doc.Title),
		nullableString(doc.Department),
		nullableString(doc.HireDate),
		managerClause,
	)
}

// QueryPersonWithProfile tests one-to-one relationship.
func QueryPersonWithProfile() string {
	return `{
		Person {
			_docID
			name
			email
			profile {
				_docID
				bio
				avatar
				website
			}
		}
	}`
}

// QueryProfileWithPerson tests reverse one-to-one lookup.
func QueryProfileWithPerson() string {
	return `{
		Profile {
			_docID
			bio
			avatar
			website
			person {
				_docID
				name
				email
			}
		}
	}`
}

// QueryAuthorWithBooks tests one-to-many relationship.
func QueryAuthorWithBooks() string {
	return `{
		Author {
			_docID
			name
			country
			books {
				_docID
				title
				isbn
				publishYear
				genre
			}
		}
	}`
}

// QueryBookWithAuthor tests reverse one-to-many lookup.
func QueryBookWithAuthor() string {
	return `{
		Book {
			_docID
			title
			isbn
			publishYear
			genre
			author {
				_docID
				name
				country
			}
		}
	}`
}

// QueryStudentEnrollments tests many-to-many via junction.
func QueryStudentEnrollments() string {
	return `{
		Student {
			_docID
			studentId
			name
			major
			enrollments {
				_docID
				enrollmentDate
				grade
				status
				course {
					_docID
					courseCode
					title
					credits
				}
			}
		}
	}`
}

// QueryCourseEnrollments tests reverse many-to-many via junction.
func QueryCourseEnrollments() string {
	return `{
		Course {
			_docID
			courseCode
			title
			credits
			department
			enrollments {
				_docID
				enrollmentDate
				grade
				status
				student {
					_docID
					studentId
					name
					major
				}
			}
		}
	}`
}

// QueryEmployeeHierarchy tests self-referential relationship.
func QueryEmployeeHierarchy() string {
	return `{
		Employee {
			_docID
			employeeId
			name
			title
			department
			manager {
				_docID
				employeeId
				name
				title
			}
			directReports {
				_docID
				employeeId
				name
				title
			}
		}
	}`
}

// QueryEmployeesByDepartment filters employees by department.
func QueryEmployeesByDepartment(department string) string {
	return fmt.Sprintf(`{
		Employee(filter: {department: {_eq: %q}}) {
			_docID
			employeeId
			name
			title
			manager {
				name
				title
			}
		}
	}`, department)
}
