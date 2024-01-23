# Grant/Group Specific Queries


## A2J

  ```py
  a = A2J(t1 = '2023-12-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True)
  ```

::: script.nice_sql.A2J
    options:
      heading_level: 3
      show_root_toc_entry: false
      show_root_members_full_path: false

### Running Reports
  ```py
  a = A2J(t1 = '2023-12-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True)
  a.run_report(a.report_funcs)
  ```
#### report_funcs
- **CASES_STARTED:** `("cases", ("ended",))`
- **CASES_ENDED:** `("cases", ("started",))`
- **LINKAGES:** `("linkages", ())`

## JAC and IDHS

  ```py
  j = JAC_IDHS(t1 = '2023-10-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True, grant = "jac")
  ```

::: script.nice_sql.JAC_IDHS
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_members_full_path: false

### Running Reports
  ```py
  j = JAC_IDHS(t1 = '2023-12-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True, grant = 'jac')
  j.run_report(j.JAC_Smartsheet)
  j.run_report(j.CVI)
  j.run_report(j.PPR)
  ```

#### JAC_Smartsheet
- **participant count:** `("idhs_tally",())`
- **genders:** `('dem_race_gender', (False,'gender',))`
- **races:** `('dem_race_gender', (False, 'race',))`
- **ages:** `('dem_age',())`
- **services:** `('idhs_service_tally',())`
- **avg hours spent on CM:** `('jac_cm_hours',())`
- **avg number of CM sessions:** `('jac_cm_sessions',())`
- **transportation assistance:** `('jac_transpo_assist',())`
- **connected to other providers:** `('jac_linked_participant_tally', ())`
- **referrals:** `('idhs_linkages',())`

#### CVI

- **total participants:** `('idhs_tally',())`
- **ages:** `('idhs_ages',())`
- **races:** `('idhs_race_gender', ('race',))`
- **genders:** `('idhs_race_gender', ('gender',))`
- **languages:** `('idhs_language',())`
- **services:** `('idhs_service_tally',())`
- **referrals:** `('idhs_linkages',())`
- **number of mediations:** `('mediation_tally',())`
- **CPIC notifications:** `('idhs_incidents_detailed', (True,))`
- **non-CPIC notifications:** `('idhs_incidents_detailed', (False,))`
- **time spent on mediations:** `('mediation_time_spent',())`

#### PPR

- **new and continuing clients:** `('idhs_tally', (False,))`
- **closed clients:** `('idhs_tally', (True,))`
- **outreach and legal:** `('idhs_service_tally',())`
- **race:** `('dem_race_gender', (False, 'race',))`
- **gender:** `('dem_race_gender', (False, 'gender',))`
- **ages:** `('idhs_ages', (True,))`
- **eligibility:** `('idhs_class_notes',(True,))`
- **cm linkages:** `('idhs_linkages',())`
- **initial mediations:** `('mediation_tally', (False,))`
- **followup mediations:** `('mediation_tally', (True,))`
- **incidents:** `('incident_tally',())`


## Monthly Reports

  ```py
  m = Monthly_Report(t1 = '2023-12-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True)
  ```

::: script.nice_sql.Monthly_Report
    options:
      heading_level: 3
      show_root_heading: False
      show_root_toc_entry: false
      show_root_members_full_path: false
### Running Reports
  ```py
   m = Monthly_Report(t1 = '2023-12-01', t2 = '2023-12-31', print_SQL = True, 
  clipboard = True)
  m.run_report(program_grant_funcs)
  m.run_report(legal_funcs)
  m.run_report(cm_outreach_funcs)
  ```

#### program_grant_funcs

- **grant involvement:** `("grant_tally", ())`
- **grants started:** `("grant_tally", ("start_date",))`
- **grants ended:** `("grant_tally", ("end_date",))`
- **Program Involvement by Days - All:** `("grouped_by_time", ("program", False, True,))`
- **Program Involvement by Days - Closed:** `("grouped_by_time", ("program", True, True,))`
- **PROGRAMS BY RACE:** `("programs_by_demographic",("race", True,))`
- **PROGRAMS BY GENDER:** `("programs_by_demographic",("gender", True, ))`
- **PROGRAMS BY AGE:** `("programs_by_demographic",("age", True,))`
- **ALL_DAYS_SERVED_BY_GRANT:** `("grouped_by_time", ("grant", False,))`
- **CLOSED_DAYS_SERVED_BY_GRANT:** `("grouped_by_time", ("grant", True,))`
- **PACKAGE OF SERVICES:** `("programs_packages",())`

#### legal_funcs

- **CiviCore.Legal Tally:** `("legal_tally",())`
- **Case Pending Tally:** `("legal_tally",(True,))`
- **Case Type - All Cases:** `("legal_case_type", (False, False, "opened", True, "case"))`
- **Case Type - Opened:** `("legal_case_type", (False, True, "opened","case"))`
- **Case Type - Closed:** `("legal_case_type", (False, True, "closed","case"))`
- **Rearrested:** `("legal_rearrested",())`
- **Pending Cases:** `("legal_pending_cases", ("t2",))`
- **Case Outcomes:** `("legal_case_outcomes", ())`
- **Guilty Plea Outcomes:** `("legal_plea_outcomes", (True,))`

#### cm_outreach_funcs

- **receiving CM:** `("programs_service_tally", ("service_cm",))`
- **Had CM Session in Month:** `("programs_sessions", ())`
- **CM Session Count:** `("programs_session_tally", ("cm",))`
- **CM Session Length:** `("programs_session_length", ())`
- **CM Session Length (Grouped):** `("programs_session_length", ("cm", True, True))`
- **Had Outreach Session in Month:** `("programs_sessions", ("outreach",))`
- **Outreach Session Count:** `("programs_session_tally", ("outreach",))`
- **linkage tally:** `("link_tally", (True, True, "linked_date",))`
- **services linked:** `("link_goal_area", (True, True, "both",))`
