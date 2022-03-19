{{
                      config(
                        database='DEV_PG_RDS_REPLICATION',
                        schema='PRODUCT_SCORECARD',
                        alias='FULL_LITE_REG',
                        materialized='incremental',
                        unique_keys = 'uuid',
                      )
                    }}

WITH ACCT_DEMO_STAGE AS (

SELECT a."UUID",
       a."FIRST",
       a."LAST",
       a."EMAIL",
       a."BIRTH",
      data:leadUserRole::string as leadUserRole ,
      data:zip::string as zip ,
      data:consentAnySchool as consentAnySchool ,
      data:consentListSchools as consentListSchools ,
      data:consentAnyCollege as consentAnyCollege ,
      data:consentListColleges as consentListColleges,
      data:interestedMajors_top as interestedMajors_top ,
      data:intendedDegreeType as intendedDegreeType ,
      data:race as race ,
      data:gender as gender ,
      data:religion as religion ,
      data:highSchoolGradYear as highSchoolGradYear ,
      data:highSchoolGPA as highSchoolGPA ,
      data:country as country ,
      data:address1 as address1 ,
      data:city as city ,
      data:state as state ,
      data:phone as phone ,
      data:highSchool as highSchool ,
      data:highSchoolOther as highSchoolOther ,
      data:highSchoolNotListed as highSchoolNotListed ,
      data:undergraduateMajor as undergraduateMajor ,
      data:collegeGradYear as collegeGradYear ,
      data:childFirstName as childFirstName ,
      data:childLastName as childLastName ,
      data:childBirthdate as childBirthdate ,
      data:childEmail as childEmail ,
      data:intendedGraduateEnrollmentDate as intendedGraduateEnrollmentDate ,
      data:interestedPrograms_top as interestedPrograms_top ,
      data:intendedGraduateDegreeType as intendedGraduateDegreeType ,
      data:college as college ,
      data:collegeGPA as collegeGPA,
      data:undergraduateDegreeType as undergraduateDegreeType,
      data:INTENDEDTRANSFERDATE as INTENDEDTRANSFERDATE,
      d.PG_ROW_VALID_FROM_TS
  
FROM {{ref('account')}} a JOIN {{ref('demographic')}} d on a.uuid = d.uuid), 

regs as (
  
SELECT ad.*, 
CASE WHEN  leadUserRole = 'local-prospect' 
 AND  zip != '' 
 AND first != '' 
 AND last != ''
 AND email IS NOT NULL  
 AND birth IS NOT NULL THEN PG_ROW_VALID_FROM_TS

-- workplace prospect -- 
WHEN  leadUserRole = 'workplace-prospect' 
 AND  zip !=''
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL THEN PG_ROW_VALID_FROM_TS

-- other --
WHEN  leadUserRole = 'other' 
 AND  zip !=''
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL THEN PG_ROW_VALID_FROM_TS

-- k12 prospects UNITED STATES --
WHEN  leadUserRole = 'k12-prospect' 
 AND  consentAnySchool != ''
 AND  consentListSchools != '' 
 AND  country = 'United States' 
 AND first != ''
 AND last != ''  
 AND email  IS NOT NULL 
 AND birth  IS NOT NULL
 AND  zip != '' THEN PG_ROW_VALID_FROM_TS

-- k12 prospect NON-US --
WHEN  leadUserRole = 'k12-prospect' 
 AND  consentAnySchool != ''
 AND  consentListSchools != '' 
 AND  country IS NOT NULL
  AND  country != 'United States'
 AND first != ''
 AND last != ''  
 AND email  IS NOT NULL 
 AND birth  IS NOT NULL THEN PG_ROW_VALID_FROM_TS

-- adult UNITED STATES--
WHEN   leadUserRole = 'adult'
 AND  first != ''
 AND  last != ''
 AND  email IS NOT NULL
 AND  birth IS NOT NULL
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedMajors_top != ''
 AND  intendedDegreeType != ''
 AND  race != ''
 AND  gender != ''
 AND  religion != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country = 'United States'
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        

-- adult NON-US --
WHEN   leadUserRole = 'adult'
 AND   first != ''
 AND   last != ''
 AND   email IS NOT NULL
 AND  birth IS NOT NULL
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedMajors_top != ''
 AND  intendedDegreeType != ''
 AND  race != ''
 AND  gender != ''
 AND  religion != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country IS NOT NULL
  AND  country != 'United States'
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        
-- college UNITED STATES --
WHEN  leadUserRole = 'college' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  intendedDegreeType != '' 
 AND  collegeGradYear != ''
 AND  gender != ''
 AND  race != ''
 AND  religion != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country = 'United States'
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        
-- college NON-US --
WHEN  leadUserRole = 'college' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  intendedDegreeType != '' 
 AND  collegeGradYear != ''
 AND  gender != ''
 AND  race != ''
 AND  religion != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country != 'United States'
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS

-- hsparent UNITED STATES -- 
WHEN  leadUserRole = 'hsparent' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != '' 
 AND  consentListColleges != ''
 AND  interestedMajors_top != '' 
 AND  intendedDegreeType != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  childFirstName != ''
 AND  childLastName != ''
 AND  childBirthdate IS NOT NULL
 AND  childEmail IS NOT NULL 
 AND  country = 'United States'
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        
-- hsparent NON-US -- 
WHEN  leadUserRole = 'hsparent' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != '' 
 AND  consentListColleges != '' 
 AND  interestedMajors_top != '' 
 AND  intendedDegreeType != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  childFirstName != ''
 AND  childLastName != ''
 AND  childBirthdate IS NOT NULL
 AND  childEmail IS NOT NULL 
 AND  country != 'United States'
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS

-- hsstudent UNITED STATES --
 WHEN  leadUserRole = 'hsstudent' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != '' 
 AND  consentListColleges != '' 
 AND  interestedMajors_top != '' 
 AND  intendedDegreeType != ''
 AND  gender != '' 
 AND  race != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country = 'United States' 
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS

-- hsstudent NON-US -- 
WHEN  leadUserRole = 'hsstudent' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != '' 
 AND  consentListColleges != '' 
 AND  interestedMajors_top != '' 
 AND  intendedDegreeType != ''
 AND  gender != '' 
 AND  race != ''
 AND  highSchoolGradYear != ''
 AND  highSchoolGPA != ''
 AND  country != 'United States' 
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        
-- graduate UNITED STATES --
WHEN  leadUserRole = 'graduate' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedPrograms_top != ''
 AND  intendedGraduateEnrollmentDate != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  intendedGraduateDegreeType!= '' 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  undergraduateDegreeType != ''
 AND  collegeGradYear != ''
 AND  collegeGPA != ''
 AND  highSchoolGradYear != '' 
 AND  highSchoolGPA != ''
 AND  country = 'United States' 
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS

-- graduate NON-US --
WHEN  leadUserRole = 'graduate' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedPrograms_top != ''
 AND  intendedGraduateEnrollmentDate != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  intendedGraduateDegreeType!= '' 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  undergraduateDegreeType != ''
 AND  collegeGradYear != ''
 AND  collegeGPA != ''
 AND  highSchoolGradYear != '' 
 AND  highSchoolGPA != ''
 AND  country != 'United States' 
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
        
-- transfer UNITED STATES --
WHEN  leadUserRole = 'transfer' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedMajors_top != '' 
 AND  intendedTransferDate != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  intendedDegreeType != '' 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  undergraduateDegreeType != '' 
 AND  collegeGradYear != ''
 AND  collegeGPA != ''
 AND  highSchoolGradYear != '' 
 AND  highSchoolGPA != ''
 AND  country = 'United States' 
 AND  address1 != ''
 AND  city != ''
 AND  state != ''
 AND  zip != ''
 AND  phone != ''
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS


-- transfer NON-US --
WHEN  leadUserRole = 'transfer' 
 AND first != ''
 AND last != ''
 AND email IS NOT NULL 
 AND birth IS NOT NULL 
 AND  consentAnyCollege != ''
 AND  consentListColleges != ''
 AND  interestedMajors_top != '' 
 AND  intendedTransferDate != ''
 AND  gender != '' 
 AND  race != ''
 AND  religion != '' 
 AND  intendedDegreeType != '' 
 AND  college != ''
 AND  undergraduateMajor != ''
 AND  undergraduateDegreeType != '' 
 AND  collegeGradYear != ''
 AND  collegeGPA != ''
 AND  highSchoolGradYear != '' 
 AND  highSchoolGPA != ''
 AND  country != 'United States' 
 AND ( highSchool IS NOT NULL 
 OR  highSchoolOther !='' and  highSchoolNotListed IS NOT NULL) THEN PG_ROW_VALID_FROM_TS
//
ELSE null
END as full_reg_converted_ts, 
  
CASE WHEN email is not null
          AND first is null
          AND last is null
          AND birth is null THEN PG_ROW_VALID_FROM_TS
ELSE NULL
END AS lite_reg_converted_ts
//

FROM ACCT_DEMO_STAGE ad)

SELECT 
      b."UUID",
      b."FIRST",
      b."LAST",
      b."EMAIL",
      b."BIRTH",
      b.leadUserRole ,
      b.zip ,
      b.consentAnySchool ,
      b.consentListSchools ,
      b.consentAnyCollege ,
      b.consentListColleges,
      b.interestedMajors_top ,
      b.intendedDegreeType ,
      b.race ,
      b.gender ,
      b.religion ,
      b.highSchoolGradYear ,
      b.highSchoolGPA ,
      b.country ,
      b.address1 ,
      b.city ,
      b.state ,
      b.phone ,
      b.highSchool ,
      b.highSchoolOther ,
      b.highSchoolNotListed ,
      b.undergraduateMajor ,
      b.collegeGradYear ,
      b.childFirstName ,
      b.childLastName ,
      b.childBirthdate ,
      b.childEmail ,
      b.intendedGraduateEnrollmentDate ,
      b.interestedPrograms_top ,
      b.intendedGraduateDegreeType ,
      b.college ,
      b.collegeGPA,
      b.undergraduateDegreeType,
      b.INTENDEDTRANSFERDATE,
      b.PG_ROW_VALID_FROM_TS,
    --   b.full_reg_converted_ts,
    --   b.lite_reg_converted_ts
      IFNULL(c.full_reg_converted_ts, b.full_reg_converted_ts) as full_reg_converted_ts,
      IFNULL(c.lite_reg_converted_ts, b.lite_reg_converted_ts) as lite_reg_converted_ts
      
FROM regs b 
left join "DEV_PG_RDS_REPLICATION"."PRODUCT_SCORECARD"."FULL_LITE_REG" c
on b.uuid = c.uuid