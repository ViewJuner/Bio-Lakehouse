CREATE DATABASE IF NOT EXISTS biolake CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE biolake;

CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL UNIQUE,
  full_name VARCHAR(128) NOT NULL,
  role VARCHAR(32) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE patients (
  patient_id VARCHAR(16) PRIMARY KEY,
  full_name VARCHAR(128) NOT NULL,
  age INT NOT NULL,
  sex CHAR(1) NOT NULL,
  risk_group VARCHAR(32) NOT NULL,
  notes VARCHAR(255)
);

CREATE TABLE devices (
  device_id VARCHAR(16) PRIMARY KEY,
  patient_id VARCHAR(16) NOT NULL,
  model VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE patient_latest (
  patient_id VARCHAR(16) PRIMARY KEY,
  device_id VARCHAR(16),
  ts DATETIME(3),
  heart_rate DOUBLE,
  spo2 DOUBLE,
  systolic_bp DOUBLE,
  diastolic_bp DOUBLE,
  temperature DOUBLE,
  risk_score DOUBLE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE vitals_agg (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  patient_id VARCHAR(16) NOT NULL,
  window_start DATETIME(3) NOT NULL,
  window_end DATETIME(3) NOT NULL,
  avg_hr DOUBLE,
  max_hr DOUBLE,
  min_spo2 DOUBLE,
  avg_sbp DOUBLE,
  sample_count INT,
  UNIQUE KEY uq_gold (patient_id, window_start)
);

CREATE TABLE alerts (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  patient_id VARCHAR(16) NOT NULL,
  device_id VARCHAR(16),
  ts DATETIME(3) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_name VARCHAR(64) NOT NULL,
  message VARCHAR(255) NOT NULL,
  heart_rate DOUBLE,
  spo2 DOUBLE,
  systolic_bp DOUBLE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_alerts_ts (ts),
  KEY idx_alerts_patient (patient_id)
);

INSERT INTO users (username, full_name, role) VALUES
  ('admin', 'Quản trị viên', 'admin'),
  ('bacsi', 'BS. Nguyễn Minh', 'clinician');

INSERT INTO patients (patient_id, full_name, age, sex, risk_group, notes) VALUES
  ('P001', 'Trần Văn An', 54, 'M', 'theo_doi', 'Tăng huyết áp nhẹ'),
  ('P002', 'Lê Thị Bình', 61, 'F', 'theo_doi', 'Đái tháo đường type 2'),
  ('P003', 'Phạm Quốc Cường', 47, 'M', 'thap', 'Sức khỏe ổn'),
  ('P004', 'Hoàng Thị Dung', 72, 'F', 'cao', 'Suy tim độ 2'),
  ('P005', 'Ngô Minh Em', 39, 'M', 'demo', 'Bệnh nhân demo nhịp tim tăng định kỳ');

INSERT INTO devices (device_id, patient_id, model, status) VALUES
  ('W001', 'P001', 'CardioBand X1', 'active'),
  ('W002', 'P002', 'CardioBand X1', 'active'),
  ('W003', 'P003', 'CardioBand Lite', 'active'),
  ('W004', 'P004', 'CardioBand Pro', 'active'),
  ('W005', 'P005', 'CardioBand X1', 'active');
