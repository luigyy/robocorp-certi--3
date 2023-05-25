*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Library             RPA.Tables
Library             Collections
Resource            shared.robot


*** Variables ***
${COUNTRY_KEY}=     SpatialDim
${GENDER_KEY}=      Dim1
${RATE_KEY}=        NumericValue
${YEAR_KEY}=        TimeDim


*** Tasks ***
Produce traffic data work items
    Download raw data
    ${traffic_data}=    Load traffic data as a table
    ${filtered_data}=    Filter and sort traffic data    ${traffic_data}
    ${latest_data_by_country}=    Get latest data by country    ${filtered_data}
    ${payloads}=    Create work items payloads    ${latest_data_by_country}
    Save work item payloads    ${payloads}

    # Write table to CSV    ${latest_data_by_country}    test.csv


*** Keywords ***
Download raw data
    Download
    ...    https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
    ...    ${OUTPUT_DIR}${/}traffic.json
    ...    overwrite=True

Load traffic data as a table
    ${json}=    Load JSON from file    ${OUTPUT_DIR}${/}traffic.json
    ${table}=    Create Table    ${json}[value]
    RETURN    ${table}

Filter and sort traffic data
    [Arguments]    ${table}
    ${max_rate}=    Set Variable    ${5.0}
    ${rate_key}=    Set Variable    NumericValue
    ${gender_key}=    Set Variable    Dim1
    ${both_genders}=    Set Variable    BTSX
    ${year_key}=    Set Variable    TimeDim
    Filter Table By Column    ${table}    ${rate_key}    <    ${max_rate}
    Filter Table By Column    ${table}    ${gender_key}    ==    ${both_genders}
    RETURN    ${table}

Get latest data by country
    [Arguments]    ${table}
    ${country_key}=    Set Variable    SpatialDim
    ${grouped_table}=    Group Table By Column    ${table}    ${country_key}
    ${latest_data_by_country}=    Create List
    FOR    ${group}    IN    @{grouped_table}
        ${first_row}=    Pop Table Row    ${group}
        Append To List    ${latest_data_by_country}    ${first_row}
    END
    RETURN    ${latest_data_by_country}

Create work items payloads
    [Arguments]    ${latest_data_by_country}
    ${payloads}=    Create List
    FOR    ${element}    IN    @{latest_data_by_country}
        ${payload}=
        ...    Create Dictionary
        ...    country=${element}[SpatialDim]
        ...    year=${element}[TimeDim]
        ...    rate=${element}[NumericValue]
        Append To List    ${payloads}    ${payload}
    END
    RETURN    ${payloads}

Save work item payloads
    [Arguments]    ${payloads}
    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

Save work item payload
    [Arguments]    ${payload}
    ${var}=    Create Dictionary    ${WORK_ITEM_NAME}=${payload}
    Create Output Work Item    variables=${var}    save=True
