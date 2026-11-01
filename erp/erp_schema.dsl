# IIT-KGP-style ERP: schema model (single source of truth for the diagrams, the DDL and the policy).
# Syntax:   table <schema>.<name> | <one-line purpose>
#             <column> <type> [pk] [uniq] [null] [sens] [fk:<schema>.<table>[.<col>]]
# `sens` marks columns that hold secrets or personal data that NatQuery must never expose.
# Types follow PostgreSQL. A foreign key without .<col> points at the target table's first primary-key column.

# =============================================================== auth: who can log in, as what
table auth.account | One login per person per hat (a person can hold a student login and a staff login: "Switch Login")
  account_id serial pk
  login_id varchar(20) uniq
  person_id int fk:people.person
  account_type varchar(10)
  password_hash text sens
  passphrase_hash text sens null
  campus_net_password_hash text sens null
  status varchar(10)
  failed_attempts smallint
  locked_until timestamptz null
  last_login_at timestamptz null
  created_at timestamptz

table auth.role | The roles the ERP knows (student, faculty, hod ...)
  role_id serial pk
  role_code varchar(30) uniq
  description text
  priority smallint

table auth.account_role | Which roles an account holds, optionally limited to one department, hall or program
  account_role_id serial pk
  account_id int fk:auth.account
  role_id int fk:auth.role
  scope_type varchar(12)
  scope_id int null
  valid_from date
  valid_to date null

table auth.session | Live login sessions (the ERP carries an ssoToken in every URL)
  session_id bigserial pk
  account_id int fk:auth.account
  token_hash text sens
  created_at timestamptz
  expires_at timestamptz
  ip_addr inet null

table auth.otp | One-time passwords for institute e-mail reset and passphrase retrieval
  otp_id bigserial pk
  account_id int fk:auth.account
  purpose varchar(20)
  otp_hash text sens
  expires_at timestamptz
  used_at timestamptz null

table auth.login_audit | Every login, failure, logout and switch-login
  audit_id bigserial pk
  account_id int fk:auth.account null
  login_id_tried varchar(20)
  event varchar(20)
  ip_addr inet null
  user_agent text null
  at timestamptz

# =============================================================== org: the institute's structure
table org.department | Academic departments
  department_id serial pk
  dept_code varchar(6) uniq
  name text
  hod_employee_id int fk:people.employee null
  is_active boolean

table org.program | Degree programmes (B.Tech, Dual Degree, M.Sc., M.Tech, Ph.D.)
  program_id serial pk
  program_code varchar(10) uniq
  name text
  level varchar(4)
  duration_years smallint
  department_id int fk:org.department

table org.hall | Halls of residence
  hall_id serial pk
  hall_code varchar(10) uniq
  name text
  warden_employee_id int fk:people.employee null
  capacity int

table org.hall_room | Rooms in a hall
  room_id serial pk
  hall_id int fk:org.hall
  room_no varchar(12)
  capacity smallint

table org.academic_session | Autumn, Spring and Summer Quarter of each academic year
  session_id serial pk
  acad_year varchar(9)
  term varchar(8)
  start_date date
  end_date date
  is_current boolean

# =============================================================== people: identity and personal records
table people.person | Name, birth and general details common to students and staff
  person_id serial pk
  full_name text
  name_hindi text null
  hindi_name_verified boolean
  gender char(1)
  date_of_birth date
  blood_group varchar(4) null
  religion varchar(20) null sens
  nationality varchar(20)
  mother_tongue varchar(20) null
  marital_status varchar(10) null
  photo_path text null
  personal_email text sens
  institute_email text uniq
  mobile_no varchar(15) sens

table people.person_identity | Government and national identifiers
  person_id int pk fk:people.person
  aadhaar_no varchar(12) sens
  abc_id varchar(12) sens null
  nad_id varchar(20) sens null

table people.bank_account | Bank details used for refunds and scholarships
  bank_account_id serial pk
  person_id int fk:people.person
  bank_name text
  branch_name text
  ifsc varchar(11)
  account_no varchar(20) sens
  aadhaar_linked boolean
  is_primary boolean

table people.address | Permanent, mailing and guardian addresses
  address_id serial pk
  person_id int fk:people.person
  address_type varchar(20)
  address_line text
  village_town_city text
  police_station text null
  pin_code varchar(6)
  district text
  state text
  country text

table people.emergency_contact | Who to call in an emergency
  emergency_contact_id serial pk
  person_id int fk:people.person
  contact_name text
  relationship varchar(20)
  contact_no varchar(15) sens
  address text null

table people.employee | Faculty and staff
  employee_id serial pk
  emp_code varchar(10) uniq
  person_id int fk:people.person uniq
  department_id int fk:org.department
  designation text
  employee_type varchar(10)
  display_name text
  official_email text
  joined_on date
  is_active boolean

table people.student | One row per enrolled student
  student_id serial pk
  roll_no varchar(12) uniq
  person_id int fk:people.person uniq
  guid varchar(10) uniq
  program_id int fk:org.program
  department_id int fk:org.department
  curriculum_id int fk:acad.curriculum
  admission_year smallint
  nature_of_admission varchar(10)
  category varchar(6)
  sub_category text null
  pwd_status text null sens
  native_state text null
  hall_id int fk:org.hall null
  room_id int fk:org.hall_room null
  hall_attached boolean
  allotted_eaa varchar(30) null
  current_semester smallint
  current_status varchar(14)
  smartcard_issued_on date null
  year_of_pass smallint null

table people.guardian | Parents and guardians of a student
  guardian_id serial pk
  student_id int fk:people.student
  relation varchar(10)
  name text
  profession text null
  education text null
  mobile_no varchar(15) sens null
  landline_no varchar(15) null
  email text sens null

table people.student_status_history | Every status a student has had (ON ROLL, DE-REGISTERED, GRADUATED ...)
  status_history_id serial pk
  student_id int fk:people.student
  status varchar(14)
  active_from date
  active_to date null
  remark text null

table people.faculty_advisor | Which teacher advises which student, and for how long
  advisor_assignment_id serial pk
  student_id int fk:people.student
  employee_id int fk:people.employee
  from_session_id int fk:org.academic_session
  to_session_id int fk:org.academic_session null

# =============================================================== acad: courses, registration, results
table acad.curriculum | A curriculum version of a programme (2020 Curricula, 2024 Onwards ...)
  curriculum_id serial pk
  name text
  program_id int fk:org.program
  effective_from_year smallint
  is_active boolean

table acad.course | The subject catalogue
  course_id serial pk
  course_code varchar(8) uniq
  title text
  credits smallint
  lecture_hours smallint
  tutorial_hours smallint
  practical_hours smallint
  department_id int fk:org.department

table acad.curriculum_course | Which course sits in which semester of which curriculum, and as what (core, depth, breadth, minor)
  curriculum_course_id serial pk
  curriculum_id int fk:acad.curriculum
  course_id int fk:acad.course
  semester_no smallint
  category varchar(12)

table acad.course_offering | A course offered in a session
  offering_id serial pk
  course_id int fk:acad.course
  session_id int fk:org.academic_session
  section varchar(3)
  seats smallint
  status varchar(10)

table acad.offering_instructor | Teachers of an offering
  offering_instructor_id serial pk
  offering_id int fk:acad.course_offering
  employee_id int fk:people.employee
  role varchar(10)

table acad.timetable_slot | When and where an offering meets
  slot_id serial pk
  offering_id int fk:acad.course_offering
  day_of_week smallint
  slot_code varchar(4)
  start_time time
  end_time time
  room_no varchar(12)
  building varchar(20) null

table acad.registration | A student's registration in an offering (regular, late, supplementary, extra credit, summer, minor, breadth)
  registration_id serial pk
  student_id int fk:people.student
  offering_id int fk:acad.course_offering
  reg_type varchar(14)
  status varchar(12)
  registered_at timestamptz

table acad.registration_card | Registration and de-registration cards generated per session
  card_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  card_type varchar(6)
  generated_at timestamptz

table acad.result | Marks and grade of one registration
  result_id serial pk
  registration_id int fk:acad.registration uniq
  marks numeric(5,2) null
  grade varchar(2) null
  grade_points smallint null
  is_backlog boolean
  declared_at timestamptz null

table acad.semester_performance | SGPA and CGPA after each session ("Performance")
  performance_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  sgpa numeric(4,2)
  cgpa numeric(4,2)
  credits_earned smallint

table acad.assignment_mark | Marks for assignments, entered by the teacher
  assignment_mark_id serial pk
  registration_id int fk:acad.registration
  title text
  max_marks numeric(5,2)
  marks_obtained numeric(5,2) null
  entered_by_employee_id int fk:people.employee
  entered_at timestamptz

table acad.class_attendance | Attendance per class
  class_attendance_id bigserial pk
  registration_id int fk:acad.registration
  attended_on date
  status char(1)
  marked_by_employee_id int fk:people.employee

table acad.exam_schedule | Central and departmental examination timetable
  exam_id serial pk
  offering_id int fk:acad.course_offering
  exam_type varchar(8)
  scope varchar(12)
  exam_date date
  start_time time
  venue text

table acad.exam_attendance | Who sat which exam
  exam_attendance_id bigserial pk
  exam_id int fk:acad.exam_schedule
  student_id int fk:people.student
  seat_no varchar(10) null
  status char(1)

table acad.admit_card | Admit cards generated and downloaded
  admit_card_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  exam_type varchar(8)
  generated_at timestamptz
  downloaded_at timestamptz null

table acad.special_program_application | Minor, micro credit, SAIP, SAPP and vertical choice applications
  application_id serial pk
  student_id int fk:people.student
  program_type varchar(12)
  target_department_id int fk:org.department null
  status varchar(10)
  applied_at timestamptz
  decided_at timestamptz null
  remarks text null

table acad.program_switch_application | Branch change, inter-disciplinary switch-over, dual degree and petroleum switch-over
  switch_id serial pk
  student_id int fk:people.student
  switch_type varchar(16)
  from_department_id int fk:org.department
  to_department_id int fk:org.department
  cgpa_at_application numeric(4,2)
  status varchar(10)
  applied_at timestamptz

table acad.breadth_choice | Breadth course preferences and what was allotted
  breadth_choice_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  course_id int fk:acad.course
  preference_no smallint
  allotted boolean

table acad.convocation_registration | Registration for convocation
  convocation_id serial pk
  student_id int fk:people.student
  convocation_year smallint
  status varchar(10)
  registered_at timestamptz

table acad.graduation_clearance | Final graduation processing: every section must clear the student
  clearance_id serial pk
  student_id int fk:people.student uniq
  department_clear boolean
  library_clear boolean
  hall_clear boolean
  accounts_clear boolean
  status varchar(10)
  updated_at timestamptz

table acad.award_prize | Awards and prizes (UG)
  award_id serial pk
  student_id int fk:people.student
  award_name text
  awarded_year smallint
  details text null

table acad.student_activity | Extra academic activities and activity choices
  activity_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  activity_type varchar(20)
  activity_name text
  hours smallint null
  verified_by_employee_id int fk:people.employee null

table acad.course_feedback | Feedback a student gives on a course
  feedback_id serial pk
  registration_id int fk:acad.registration uniq
  rating smallint
  comments text null
  submitted_at timestamptz

table acad.medical_leave | Medical leave applications
  medical_leave_id serial pk
  student_id int fk:people.student
  from_date date
  to_date date
  reason text sens
  status varchar(10)
  approved_by_employee_id int fk:people.employee null

table acad.student_document | Uploaded supporting documents and GATE score
  document_id serial pk
  student_id int fk:people.student
  doc_type varchar(20)
  file_path text
  gate_score smallint null
  uploaded_at timestamptz
  verified boolean

table acad.rep_nomination | Student representative nominations
  nomination_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  post text
  status varchar(10)

table acad.data_sharing_consent | Consent for sharing data outside the ERP
  student_id int pk fk:people.student
  consented boolean
  decided_at timestamptz

# =============================================================== fin: fees, scholarships, claims, salary
table fin.fee_head | Kinds of charges (tuition, hall rent, mess, convocation ...)
  head_id serial pk
  name text uniq
  category varchar(12)

table fin.fee_structure | Standard amount of each head per programme per year
  fee_structure_id serial pk
  program_id int fk:org.program
  acad_year varchar(9)
  head_id int fk:fin.fee_head
  amount numeric(10,2)

table fin.fee_demand | What a student has to pay in a session
  demand_id serial pk
  student_id int fk:people.student
  session_id int fk:org.academic_session
  total_amount numeric(10,2)
  due_date date
  status varchar(10)

table fin.fee_demand_item | Line items of a demand
  demand_item_id serial pk
  demand_id int fk:fin.fee_demand
  head_id int fk:fin.fee_head
  amount numeric(10,2)

table fin.payment | Payments made against a demand
  payment_id serial pk
  demand_id int fk:fin.fee_demand
  student_id int fk:people.student
  amount numeric(10,2)
  mode varchar(8)
  gateway_txn_id varchar(30) uniq
  status varchar(10)
  paid_at timestamptz
  receipt_no varchar(16) uniq

table fin.family_income_declaration | Family income declared for fee waiver and scholarships
  declaration_id serial pk
  student_id int fk:people.student
  acad_year varchar(9)
  annual_income numeric(12,2) sens
  income_source text sens
  verified_by_employee_id int fk:people.employee null
  declared_at timestamptz

table fin.scholarship_scheme | Scholarship schemes
  scheme_id serial pk
  name text
  provider text
  min_cgpa numeric(4,2) null
  max_family_income numeric(12,2) null

table fin.scholarship_award | Scholarships awarded to students
  award_id serial pk
  student_id int fk:people.student
  scheme_id int fk:fin.scholarship_scheme
  session_id int fk:org.academic_session
  amount numeric(10,2)
  status varchar(10)
  disbursed_at timestamptz null

table fin.mediclaim_claim | Medical bill claims
  claim_id serial pk
  student_id int fk:people.student
  hospital text
  bill_amount numeric(10,2) sens
  claimed_amount numeric(10,2) sens
  status varchar(10)
  submitted_at timestamptz
  settled_at timestamptz null

table fin.salary_slip | Monthly salary slips of staff
  salary_slip_id serial pk
  employee_id int fk:people.employee
  pay_month date
  gross numeric(10,2) sens
  deductions numeric(10,2) sens
  net numeric(10,2) sens

# =============================================================== lib: library
table lib.book | Library catalogue
  book_id serial pk
  isbn varchar(13) null
  title text
  author text
  publisher text null
  category varchar(20)

table lib.book_copy | Physical copies of a book
  copy_id serial pk
  book_id int fk:lib.book
  accession_no varchar(12) uniq
  status varchar(10)

table lib.loan | Books issued to students and staff
  loan_id serial pk
  copy_id int fk:lib.book_copy
  person_id int fk:people.person
  issued_on date
  due_on date
  returned_on date null

table lib.library_due | Fines for late return or loss
  due_id serial pk
  loan_id int fk:lib.loan
  person_id int fk:people.person
  amount numeric(8,2)
  reason varchar(10)
  status varchar(8)

# =============================================================== svc: applications, e-Office, mail, notices
table svc.application_type | Catalogue of paperless forms (fee certificate, creche, interim grade card ...)
  type_id serial pk
  code varchar(30) uniq
  name text
  category varchar(20)

table svc.paperless_application | A form submitted through Paperless Application
  application_id serial pk
  type_id int fk:svc.application_type
  applicant_person_id int fk:people.person
  student_id int fk:people.student null
  submitted_at timestamptz
  status varchar(10)
  payload jsonb

table svc.application_step | Approval chain of an application
  step_id serial pk
  application_id int fk:svc.paperless_application
  step_no smallint
  approver_account_id int fk:auth.account
  action varchar(10)
  remarks text null
  acted_at timestamptz null

table svc.complaint | Complaints and requests
  complaint_id serial pk
  account_id int fk:auth.account
  category varchar(20)
  subject text
  description text
  status varchar(10)
  created_at timestamptz
  resolved_at timestamptz null

table svc.grievance | Grievances
  grievance_id serial pk
  account_id int fk:auth.account
  category varchar(20)
  description text
  status varchar(10)
  assigned_to_account_id int fk:auth.account null
  created_at timestamptz

table svc.mail_message | Internal ERP mail
  message_id serial pk
  from_account_id int fk:auth.account
  subject text
  body text
  sent_at timestamptz

table svc.mail_recipient | Who received a mail and whether they read it
  mail_recipient_id serial pk
  message_id int fk:svc.mail_message
  account_id int fk:auth.account
  read_at timestamptz null

table svc.notice | Apna (internal) notices
  notice_id serial pk
  title text
  body text
  posted_by_account_id int fk:auth.account
  audience varchar(10)
  posted_at timestamptz
  expires_at timestamptz null

table svc.counselling_session | Counselling Centre sessions
  session_id serial pk
  student_id int fk:people.student
  counsellor_employee_id int fk:people.employee
  scheduled_at timestamptz
  status varchar(10)
  notes text sens null

table svc.efile_category | Department e-file categories
  category_id serial pk
  department_id int fk:org.department
  name text

table svc.efile | An electronic file in a department
  efile_id serial pk
  category_id int fk:svc.efile_category
  department_id int fk:org.department
  subject text
  created_by_account_id int fk:auth.account
  status varchar(10)
  created_at timestamptz

table svc.efile_movement | Movement of an e-file from desk to desk
  movement_id serial pk
  efile_id int fk:svc.efile
  from_account_id int fk:auth.account
  to_account_id int fk:auth.account
  sent_at timestamptz
  received_at timestamptz null
  remarks text null

table svc.physical_dispatch | Physical file or item despatched or received by a department
  dispatch_id serial pk
  department_id int fk:org.department
  direction varchar(8)
  item_type varchar(10)
  description text
  party text
  ref_no varchar(20)
  dated date
  handled_by_account_id int fk:auth.account

table svc.physical_item_movement | Where a physical item is now
  item_movement_id serial pk
  dispatch_id int fk:svc.physical_dispatch
  to_account_id int fk:auth.account
  status varchar(10)
  moved_at timestamptz

# =============================================================== gh and cdc: guest house, career development cell
table gh.guesthouse | Guest houses on campus
  guesthouse_id serial pk
  name text
  location text

table gh.gh_room | Rooms of a guest house
  gh_room_id serial pk
  guesthouse_id int fk:gh.guesthouse
  room_no varchar(8)
  room_type varchar(10)
  rate_per_day numeric(8,2)

table gh.gh_booking | Guest house bookings
  booking_id serial pk
  gh_room_id int fk:gh.gh_room
  booked_by_account_id int fk:auth.account
  guest_name text
  purpose text
  check_in date
  check_out date
  status varchar(10)
  charges numeric(8,2)

table cdc.company | Recruiting companies
  company_id serial pk
  name text uniq
  sector text

table cdc.job_posting | Placement and internship postings
  posting_id serial pk
  company_id int fk:cdc.company
  role_title text
  posting_type varchar(10)
  ctc_lakhs numeric(6,2) sens
  min_cgpa numeric(4,2) null
  deadline date

table cdc.job_application | Student applications to postings
  job_application_id serial pk
  posting_id int fk:cdc.job_posting
  student_id int fk:people.student
  status varchar(10)
  applied_at timestamptz

table cdc.offer | Offers made to students
  offer_id serial pk
  job_application_id int fk:cdc.job_application uniq
  ctc_offered_lakhs numeric(6,2) sens
  offered_on date
  accepted boolean null

# =============================================================== sys: menus and audit
table sys.menu_module | Top-level modules (Academic, Accounts, Guest House, CDC)
  module_id serial pk
  name text uniq

table sys.menu_item | Every menu link in the ERP
  menu_id serial pk
  module_id int fk:sys.menu_module
  group_name text
  title text
  url text
  description text null
  sort_order smallint
  is_active boolean

table sys.role_menu_access | Which role sees which menu link
  role_id int pk fk:auth.role
  menu_id int pk fk:sys.menu_item

table sys.audit_event | Data-change audit trail
  event_id bigserial pk
  account_id int fk:auth.account null
  table_name text
  record_pk text
  action varchar(8)
  at timestamptz
