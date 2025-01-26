# cms_claims

One of the major challenges many new data engineers and analysts face when trying to build data projects is finding data sets. In particular, many data sets aren't really the data sets don't tie to specific domains.

That's why I have set-up the data set below. The core data set is the Center For Medicare and Medicaid Services Synthetic data set. That is to say, in their own words, realistic-but-not-real data.

The CMS created these synthetic datasets to allow interested parties to gain familiarity with using Medicare claims data while protecting beneficiary privacy. The Synthetic Medicare Claims data were designed to map to CMS’ Research Identifiable File (RIF) format, meaning that even though they are not tied to any real patient data, they mimic the real claims data that CMS makes available to researchers through the CMS Chronic Conditions Warehouse (CCW).

So this data set will give you all the joys and pains of working with real data.

It has codes and IDs that require translating, it has missing dimesions that I have found such as provider and location data.

It also provides plenty of unique possibilites of projects.

<img width="594" alt="Screen Shot 2024-11-20 at 1 38 34 PM" src="https://github.com/user-attachments/assets/213024d5-5b12-4471-ba7d-656df5d89b41">


## Data Sets

As a data engineer we often need to spend time piecing together data sets that might not all come from one source. Well we are in luck.

There are two different synthetic CMS data sets, one from 2008-2010 and the other from 2015-2023. The first is decently large(in the millions) has multiple files but is older and in turn requires usage of ICD where as the second is considerably smaller.

But two different schema formats, needing to look for other data sets across the internet, guess what you've got yourself a data engineering project that will let you know what it feels like to be a data engineer.

Here is how the data sets are broken down.

- Claims but for CMS they might call this an Encounter (Inpatient, Outpatient, etc)
- [Claims 2008-2010 Data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1)
- [Claims 2015-2025 Data](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event)
- Beneficiary - I reference this as patients as it's what I called it when working in claims 
- Provider
- Prescriptions
- NDC - Drug descriptions
- Location broken down by county
- 
<img width="932" alt="Screen Shot 2024-11-20 at 2 06 41 PM" src="https://github.com/user-attachments/assets/df1023d8-8aab-4462-9090-a35a0f4b63fb">

<img width="598" alt="Screen Shot 2024-11-20 at 1 45 49 PM" src="https://github.com/user-attachments/assets/84349c39-ec2e-4812-bc8f-546346b54d02">

## System Design

The overall goal of the system that is being built is that it can easily accomodate new companies data sets. So instead of having to build a new pipeline every time, you'd simply create a new transform script that standardizing the new data to fit the expected output. From their any data product would simply ingest the new data without any issues.

<img width="1192" alt="Screen Shot 2024-12-06 at 7 04 03 PM" src="https://github.com/user-attachments/assets/da196973-17a6-4d1e-ac7b-7c62e029e4cf">


## Project Ideas
Here’s a more specific list of potential projects based on individual diagnoses, treatments, and claim patterns. These include opioid-related projects as well as other diagnosis-specific ideas.

---

### **Opioid-Related Projects**

1. **Opioid Overprescription Detection**
   - **Objective**: Identify providers who prescribe opioids more frequently than peers for similar diagnoses.
   - **Approach**:
     - Compare opioid-related HCPCS codes across providers for diagnoses like back pain or minor injuries.
     - Include geographic and demographic factors for context.

2. **Inappropriate Opioid Prescriptions**
   - **Objective**: Detect instances where opioids are prescribed for diagnoses not typically requiring them.
   - **Approach**:
     - Cross-reference opioid prescriptions with ICD codes (e.g., prescribing opioids for sprains or headaches).
     - Highlight outliers for further investigation.

3. **Tracking Opioid Treatment Pathways**
   - **Objective**: Understand how often opioid prescriptions escalate into long-term treatments.
   - **Approach**:
     - Track patients over time to study transitions from short-term prescriptions to chronic opioid use.
     - Analyze claim lines and HCPCS codes for recurring opioid-related visits.

---

### **Diagnosis-Specific Projects**

6. **Overuse of Imaging**
   - **Objective**: Detect excessive imaging for minor injuries.
   - **Approach**:
     - Look for diagnoses like sprains and strains paired with repeated imaging claims (e.g., 73610).
     - Highlight providers with unusually high imaging rates.

7. **Inappropriate Emergency Room Visits**
   - **Objective**: Identify claims for non-emergency conditions in ER settings.
   - **Approach**:
     - Cross-reference diagnoses and HCPCS codes with provider types and locations.
     - Highlight cases like minor injuries treated in ERs instead of urgent care.

---

### **Chronic Conditions**
8. **Diabetes Management Patterns**
   - **Objective**: Study trends in the treatment of diabetes across claims.
   - **Approach**:
     - Analyze procedures and medications associated with diabetes diagnoses.
     - Detect gaps in care, such as missing standard treatments like HbA1c tests.

9. **Heart Disease Treatment Pathways**
   - **Objective**: Analyze common pathways for heart disease patients.
   - **Approach**:
     - Study claims with heart disease diagnoses and track treatments like stents or bypass surgeries.
     - Build a model to predict the next step in a patient’s treatment.

---

### **Provider-Level Insights**
12. **Surgical Complication Rates**
    - **Objective**: Measure rates of complications for surgeries.
    - **Approach**:
      - Analyze claims with postoperative diagnoses or additional treatments for infections, bleeding, or other complications.

13. **Chronic Pain Treatment Analysis**
    - **Objective**: Study providers treating chronic pain for potential overuse of certain treatments (e.g., opioids, imaging).
    - **Approach**:
      - Correlate pain-related diagnoses with the frequency of invasive procedures or medications.

14. **Provider Specialization Consistency**
    - **Objective**: Detect providers treating conditions outside their specialty.
    - **Approach**:
      - Analyze claims for patterns where diagnoses and treatments do not align with the provider’s primary focus.

---

### **Population-Level Patterns**
15. **Pediatric Treatment Trends**
    - **Objective**: Study trends in claims for pediatric diagnoses and treatments.
    - **Approach**:
      - Focus on common pediatric issues like fractures, infections, or asthma.
      - Compare provider and treatment variability.

16. **Elderly Fall Prevention**
    - **Objective**: Identify predictors for falls among the elderly.
    - **Approach**:
      - Study claims with diagnoses like fractures or external causes related to falls (e.g., W18).
      - Suggest interventions based on patterns.

---

### **Cost and Efficiency**
17. **Duplicate Claim Detection**
    - **Objective**: Identify claims with similar diagnoses and procedures submitted multiple times.
    - **Approach**:
      - Look for matching ICD and HCPCS codes within a short time frame for the same patient.

18. **Procedure Cost Benchmarking**
    - **Objective**: Compare costs for similar procedures across providers and regions.
    - **Approach**:
      - Use claims data to benchmark average costs for common treatments.

19. **Underutilized Preventive Care**
    - **Objective**: Detect cases where preventive treatments (e.g., vaccines, screenings) are underutilized.
    - **Approach**:
      - Cross-reference claims with recommended guidelines for certain populations.

---

Would you like help prioritizing these projects or exploring the implementation details of any specific one?

## System Design

What you'll notice is we currently will be pulling in two data sets with similar entities. Claims, patients, drug events, etc. This allows us to create a single pipeline for each entity. This is how you can build your own data product. In fact, I know plenty of companies that range from ecommerce analytics and forecasting to mortgage metrics that provide third-party analytics providers that connect to APIs or ingest CSVs to provide analytics.

They do so by creating a standardized template on what they expect and then creating reporting around that.

So that's what we will do. We will take the various claim files, standardize them and create a single claim table.

There are a few ways you can do this.

1. Create a query for each entity, name it something like CUSTOMER_ENTITY_VERSION.sql, and pass that information to your data pipeline to pull the correct sql script when running.
2. Create a look-up table where you map the your standardized column names to the column sql for the file. For example, let's say you have a field named patient_id, but the data set you're pulling from requires you to do CONCAT(FIELD_1,LEFT(FIELD_5,2) for patient ID. Then that would be the value you'd put into the mapping table. Along with the version and customer ID. Also, in this case customer ID is doesn't really exist but it would be required in a larger system
3. Still another option is you could just build a data pipeline for each customer and entity. I'd avoid this path if you've got dozens of customers.

## Important Claims Concepts

It's tempting to dive straight into the data, but its important to understand what the data represents. Otherwise, you're not going to understand why there are multiple ICD codes, and what they represent. You might not understand what inpatient vs outpatient mean and why they are different.

So let's dive into some of the basics around claims data.

---

### **1. ICD Codes (International Classification of Diseases)**

**Purpose**:  
ICD codes are standardized codes used globally to classify and code diseases, symptoms, and procedures. They are critical for documenting diagnoses and conditions. But, what might get confusing is there are both procedure and diagnosis codes.

- **ICD Diagnosis Codes**: 
  - Represent the medical reason for a patient's visit or condition (e.g., diabetes, hypertension).
  - Format: **ICD-10-CM** (e.g., E11.9 for Type 2 Diabetes Mellitus without complications).

- **ICD Procedure Codes**:  
  - Represent procedures performed during inpatient hospital stays and is used for reimbursement, quality reporting, and identifying inpatient care trends.
  - Format: **ICD-10-PCS** (e.g., 0U5B7ZZ for an open hysterectomy).
---

### **2. CPT Codes (Current Procedural Terminology)**

**Purpose**:  
CPT codes describe the specific medical, surgical, and diagnostic services provided by healthcare professionals.

- **Format**: Five-digit numeric codes (e.g., 99213 for a standard outpatient office visit). Some codes also include a modifier for additional specificity.
- **Categories**:
  - **Category I**: Common procedures and services.
  - **Category II**: Performance measurement (optional reporting codes).
  - **Category III**: Emerging technologies and experimental procedures.

---

### **3. HCPCS Codes (Healthcare Common Procedure Coding System)**

**Purpose**:  
HCPCS codes expand on CPT codes to include non-physician services like durable medical equipment (DME), transportation, or drugs.

- **Level I**: Equivalent to CPT codes.
- **Level II**: Covers services and equipment not included in CPT (e.g., E0601 for a CPAP machine).

---

### **4. DRG Codes (Diagnosis-Related Groups)**

**Purpose**:  
DRG codes classify inpatient stays into groups based on diagnoses and procedures. These are used primarily for hospital reimbursement.

- Example: DRG 470 for a major joint replacement or reattachment procedure.

---

### **5. Revenue Codes**

**Purpose**:  
Revenue codes indicate the department or type of service provided (e.g., emergency room, pharmacy). They are essential for understanding claims at the departmental level.

- Example: Revenue code 450 for emergency room services.

---

### **6. NDC (National Drug Codes)**

**Purpose**:  
NDC codes are unique identifiers for drugs and are used in claims involving prescriptions or administered medications.

- Format: 10-11 digit numeric code (e.g., 0781-1506-10 for a specific vial of insulin).


---

### **Practical Use Cases for Analysts**

1. **Cost Analysis**: Identify the highest-cost services and procedures using CPT or HCPCS codes.
2. **Population Health**: Segment populations by diagnosis (ICD codes) to identify trends or disparities in care.
3. **Utilization Analysis**: Determine service utilization rates by department (revenue codes) or service type.
4. **Performance Metrics**: Evaluate healthcare provider performance based on DRG benchmarks or CPT-related outcomes.
5. **Fraud Detection**: Identify anomalies in claims data, such as duplicate billing or mismatched codes.

By mastering these concepts, data analysts can extract actionable insights from claims data, helping improve operational efficiency, financial performance, and patient outcomes.
