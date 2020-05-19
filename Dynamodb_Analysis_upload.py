import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
import json
import csv
import decimal
from init import pymysql
import MySQLdb
import os

dynamodb = boto3.resource('dynamodb')

def scan_table(table_name): #runtime: about 15min
    #record time for scan the entire table
    print(dynamodb)
    start = time.time()
    table = dynamodb.Table(table_name)

    response = table.scan(
        FilterExpression = Attr('usingS3').eq(0),
        )

    items = response['Items']

    #continue to gat all records in the table, using ExclusiveStartKey
    while True:
        print(len(response['Items']))
        if response.get('LastEvaluatedKey'):
            response = table.scan(
                ExclusiveStartKey = response['LastEvaluatedKey'],
                FilterExpression = Attr('usingS3').eq(0)
                )
            items += response['Items']
        else:           
            break
    print('execution time:', time.time() - start)
    
    return items

#call scan_table function
items = scan_table('Analysis')
print("Count Items from DynamoDB:", len(items)) 


#use a list to store (personIdentifier, number of articles for this person)
count_articles = []
#also check if there is anyone in the table with 0 article
no_article_person_list = []
for i in items:
    count_articles.append((i['reCiterFeature']['personIdentifier'], len(i['reCiterFeature']['reCiterArticleFeatures'])))
    if len(i['reCiterFeature']['reCiterArticleFeatures']) == 0:
        no_article_person_list.append(i['reCiterFeature']['personIdentifier'])
print("Count Items from count_articles list:", len(count_articles))
print(len(no_article_person_list))


outputPath = '/usr/src/app/ReCiter/'

#code for personArticle table
#open a csv file in the directory you preferred
f = open(outputPath + 'personArticle_mysql.csv','w')
#write column names into file
f.write("personIdentifier," + "pmid," + "pmcid," + "totalArticleScoreStandardized," + "totalArticleScoreNonStandardized," 
        + "userAssertion," + "publicationDateDisplay," + "publicationDateStandardized," + "publicationTypeCanonical," + "publicationAbstract,"
        + "scopusDocID," + "journalTitleVerbose," + "articleTitle," + "feedbackScoreAccepted," + "feedbackScoreRejected," + "feedbackScoreNull," 
        + "articleAuthorNameFirstName," + "articleAuthorNameLastName," + "institutionalAuthorNameFirstName," + "institutionalAuthorNameMiddleName," + "institutionalAuthorNameLastName,"
        + "nameMatchFirstScore," + "nameMatchFirstType," + "nameMatchMiddleScore," + "nameMatchMiddleType," + "nameMatchLastScore," + "nameMatchLastType," + "nameMatchModifierScore," + "nameScoreTotal,"
        + "emailMatch," + "emailMatchScore," + "journalSubfieldScienceMetrixLabel," + "journalSubfieldScienceMetrixID," + "journalSubfieldDepartment," + "journalSubfieldScore,"
        + "relationshipEvidenceTotalScore," + "relationshipMinimumTotalScore," + "relationshipNonMatchCount," + "relationshipNonMatchScore,"
        + "articleYear," + "identityBachelorYear," + "discrepancyDegreeYearBachelor," + "discrepancyDegreeYearBachelorScore," + "identityDoctoralYear,"
        + "discrepancyDegreeYearDoctoral," + "discrepancyDegreeYearDoctoralScore," + "genderScoreArticle," + "genderScoreIdentity," + "genderScoreIdentityArticleDiscrepancy,"
        + "personType," + "personTypeScore," + "countArticlesRetrieved," + "articleCountScore," 
        + "targetAuthorInstitutionalAffiliationArticlePubmedLabel," + "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore," + "scopusNonTargetAuthorInstitutionalAffiliationSource," + "scopusNonTargetAuthorInstitutionalAffiliationScore,"
        + "totalArticleScoreWithoutClustering," + "clusterScoreAverage," + "clusterReliabilityScore," + "clusterScoreModificationOfTotalScore," 
        + "datePublicationAddedToEntrez," + "clusterIdentifier," + "doi," + "issn," + "issue," + "journalTitleISOabbreviation," + "pages," + "timesCited," + "volume"
        + "\n")
#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 

for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']

        totalArticleScoreStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['totalArticleScoreStandardized']
        totalArticleScoreNonStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['totalArticleScoreNonStandardized']
        userAssertion = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['userAssertion']
        publicationDateStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationDateStandardized']
        publicationTypeCanonical = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationType']['publicationTypeCanonical']
        # example1: when you get key error, check whether the key exist in dynamodb or not
        if 'scopusDocID' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            scopusDocID = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['scopusDocID']
        else:
            scopusDocID = ""
        
        if 'pmcid' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            pmcid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmcid']
        else:
            pmcid = ""
        
        journalTitleVerbose = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['journalTitleVerbose']
        journalTitleVerbose = journalTitleVerbose.replace('"', '""')
        if 'articleTitle' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            articleTitle = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['articleTitle']
            articleTitle = articleTitle.replace('"', '""')
        else:
            articleTitle = ""

        if 'acceptedRejectedEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'feedbackScoreAccepted' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreAccepted = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreAccepted']
            else: 
                feedbackScoreAccepted = 0
            if 'feedbackScoreRejected' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreRejected = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreRejected']
            else: 
                feedbackScoreRejected = 0
            if 'feedbackScoreNull' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreNull = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreNull']
            else: 
                feedbackScoreNull = 0
        else:
            feedbackScoreAccepted, feedbackScoreRejected, feedbackScoreNull = "", "", ""
        
        if 'authorNameEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'articleAuthorName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                if 'firstName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']: 
                    articleAuthorName_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['firstName']
                else:
                    articleAuthorName_firstName = ""
                articleAuthorName_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['lastName']
            else:
                articleAuthorName_firstName, articleAuthorName_lastName = "", ""
            institutionalAuthorName_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['firstName']
            if 'middleName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']:
                institutionalAuthorName_middleName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['middleName']
            else:
                institutionalAuthorName_middleName = ""
            institutionalAuthorName_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['lastName']
            nameMatchFirstScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstScore']
            nameMatchFirstType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstType']
            nameMatchMiddleScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleScore']
            nameMatchMiddleType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleType']
            nameMatchLastScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastScore']
            nameMatchLastType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastType']
            nameMatchModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchModifierScore']
            nameScoreTotal = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameScoreTotal']
        else:
            articleAuthorName_firstName, articleAuthorName_lastName, institutionalAuthorName_firstName, institutionalAuthorName_middleName, institutionalAuthorName_lastName = "", "", "", "", ""
            nameMatchFirstScore, nameMatchFirstType, nameMatchMiddleScore, nameMatchMiddleType, nameMatchLastScore, nameMatchLastType, nameMatchModifierScore, nameScoreTotal = "", "", "", "", "", "", "", ""

        if 'emailEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            emailMatch = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatch']
            if 'false' in emailMatch:
                emailMatch = ""
            emailMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatchScore']
        else:
            emailMatch, emailMatchScore = "", 0
        
        if 'journalCategoryEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            journalSubfieldScienceMetrixLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixLabel']
            journalSubfieldScienceMetrixLabel = journalSubfieldScienceMetrixLabel.replace('"', '""')
            journalSubfieldScienceMetrixID = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixID']
            journalSubfieldDepartment = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldDepartment']
            journalSubfieldDepartment = journalSubfieldDepartment.replace('"', '""')
            journalSubfieldScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScore']
        else:
            journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore = "", "", "", 0
        
        if 'relationshipEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            relationshipEvidenceTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipEvidenceTotalScore']
            if 'relationshipNegativeMatch' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipMinimumTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipMinimumTotalScore']
                relationshipNonMatchCount = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchCount']
                relationshipNonMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchScore']
            else:
                relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0
        else:
            relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0, 0
        
        if 'educationYearEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'articleYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                articleYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['articleYear']
            else:
                articleYear = 0
            if 'identityBachelorYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityBachelorYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityBachelorYear']
            else:
                identityBachelorYear = ""
            if 'discrepancyDegreeYearBachelor' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelor']
            else:
                discrepancyDegreeYearBachelor = 0
            if 'discrepancyDegreeYearBachelorScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelorScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelorScore']
            else:
                discrepancyDegreeYearBachelorScore = 0
            if 'identityDoctoralYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityDoctoralYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityDoctoralYear']
            else:
                identityDoctoralYear = ""
            if 'discrepancyDegreeYearDoctoral' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoral = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoral']
            else:
                discrepancyDegreeYearDoctoral = 0
            if 'discrepancyDegreeYearDoctoralScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoralScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoralScore']
            else:
                discrepancyDegreeYearDoctoralScore = 0
        else:
            articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore = 0, "", 0, 0, "", 0, 0
        
        if 'genderEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            genderScoreArticle = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreArticle']
            genderScoreIdentity = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentity']
            genderScoreIdentityArticleDiscrepancy = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentityArticleDiscrepancy']
        else:
            genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy = 0, 0, 0
        
        if 'personTypeEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            personType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personType']
            personTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personTypeScore']
        else:
            personType, personTypeScore = "", 0
        
        if 'articleCountEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            countArticlesRetrieved = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['countArticlesRetrieved']
            articleCountScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['articleCountScore']
        else:
            countArticlesRetrieved, articleCountScore = 0, 0
        
        if 'pubmedTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationArticlePubmedLabel']
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = targetAuthorInstitutionalAffiliationArticlePubmedLabel.replace('"', '""')
            pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationMatchTypeScore']
        else:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = "", 0
        
        if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            scopusNonTargetAuthorInstitutionalAffiliationSource = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationSource']
            scopusNonTargetAuthorInstitutionalAffiliationScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationScore']
        else:
            scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore = "", 0

        if 'averageClusteringEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            totalArticleScoreWithoutClustering = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['totalArticleScoreWithoutClustering']
            clusterScoreAverage = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreAverage']
            clusterReliabilityScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterReliabilityScore']
            clusterScoreModificationOfTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreModificationOfTotalScore']
        else:
            totalArticleScoreWithoutClustering, clusterScoreAverage, clusterReliabilityScore, clusterScoreModificationOfTotalScore = "", "", "", ""

        if 'clusterIdentifier' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']:
            clusterIdentifier = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterIdentifier']
        else :
            clusterIdentifier = 0
        
        datePublicationAddedToEntrez = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['datePublicationAddedToEntrez']
        if 'publicationDateDisplay' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            publicationDateDisplay = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationDateDisplay']
        else:
            publicationDateDisplay = ""

        if 'doi' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            doi = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['doi']
        else: 
            doi = ""

        if 'issn' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            issn_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'])
            for k in range(issn_temp):
                issntype = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issntype']
                if issntype == 'Linking':
                    issn = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issn']
        else:
            issn = ""

        if 'issue' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            issue = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issue']
        else: 
            issue = ""
        if 'journalTitleISOabbreviation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            journalTitleISOabbreviation = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['journalTitleISOabbreviation']
            journalTitleISOabbreviation = journalTitleISOabbreviation.replace('"', '""')
        else:
            journalTitleISOabbreviation = ""
        if 'pages' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            pages = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pages']
        else:
            pages = ""
        if 'timesCited' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            timesCited = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['timesCited']
        else: 
            timesCited = 0
        if 'volume' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            volume = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['volume']
        else:
            volume = ""
        
        #write all extracted features into csv file
        #some string value may contain a comma, in this case, we need to double quote the output value, for example, '"' + str(journalSubfieldScienceMetrixLabel) + '"'
        f.write('"' + str(personIdentifier) + '"' + "," + '"' + str(pmid) + '"' + "," + '"' + str(pmcid) + '"' + "," + '"' + str(totalArticleScoreStandardized) + '"' + "," 
                + '"' + str(totalArticleScoreNonStandardized) + '"' + "," + '"' + str(userAssertion) + '"' + "," 
                + '"' + str(publicationDateDisplay) + '"' + "," + '"' + str(publicationDateStandardized) + '"' + "," + '"' + str(publicationTypeCanonical) + '"' + ","
                + '"' + str(scopusDocID) + '"' + ","  + '"' + str(journalTitleVerbose) + '"' + "," + '"' + str(articleTitle) + '"' + "," + '"' + str(feedbackScoreAccepted) + '"' + "," + '"' + str(feedbackScoreRejected) + '"' + "," + '"' + str(feedbackScoreNull) + '"' + "," 
                + '"' + str(articleAuthorName_firstName) + '"' + "," + '"' + str(articleAuthorName_lastName) + '"' + "," + '"' + str(institutionalAuthorName_firstName) + '"' + "," + '"' + str(institutionalAuthorName_middleName) + '"' + "," + '"' + str(institutionalAuthorName_lastName) + '"' + ","
                + '"' + str(nameMatchFirstScore) + '"' + "," + '"' + str(nameMatchFirstType) + '"' + "," + '"' + str(nameMatchMiddleScore) + '"' + "," + '"' + str(nameMatchMiddleType) + '"' + ","
                + '"' + str(nameMatchLastScore) + '"' + "," + '"' + str(nameMatchLastType) + '"' + "," + '"' + str(nameMatchModifierScore) + '"' + "," + '"' + str(nameScoreTotal) + '"' + ","
                + '"' + str(emailMatch) + '"' + "," + '"' + str(emailMatchScore) + '"' + "," 
                + '"' + str(journalSubfieldScienceMetrixLabel) + '"' + "," + '"' + str(journalSubfieldScienceMetrixID) + '"' + "," + '"' + str(journalSubfieldDepartment) + '"' + "," + '"' + str(journalSubfieldScore) + '"' + "," 
                + '"' + str(relationshipEvidenceTotalScore) + '"' + "," + '"' + str(relationshipMinimumTotalScore) + '"' + "," + '"' + str(relationshipNonMatchCount) + '"' + "," + '"' + str(relationshipNonMatchScore) + '"' + ","
                + '"' + str(articleYear) + '"' + "," + '"' + str(identityBachelorYear) + '"' + "," + '"' + str(discrepancyDegreeYearBachelor) + '"' + "," + '"' + str(discrepancyDegreeYearBachelorScore) + '"' + ","
                + '"' + str(identityDoctoralYear) + '"' + "," + '"' + str(discrepancyDegreeYearDoctoral) + '"' + "," + '"' + str(discrepancyDegreeYearDoctoralScore) + '"' + "," 
                + '"' + str(genderScoreArticle) + '"' + "," + '"' + str(genderScoreIdentity) + '"' + "," + '"' + str(genderScoreIdentityArticleDiscrepancy) + '"' + "," 
                + '"' + str(personType) + '"' + "," + '"' + str(personTypeScore) + '"' + ","
                + '"' + str(countArticlesRetrieved) + '"' + "," + '"' + str(articleCountScore) + '"' + ","
                + '"' + str(targetAuthorInstitutionalAffiliationArticlePubmedLabel) + '"' + "," + '"' + str(pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore) + '"' + "," + '"' + str(scopusNonTargetAuthorInstitutionalAffiliationSource) + '"' + "," + '"' + str(scopusNonTargetAuthorInstitutionalAffiliationScore) + '"' + ","
                + '"' + str(totalArticleScoreWithoutClustering) + '"' + "," + '"' + str(clusterScoreAverage) + '"' + "," + '"' + str(clusterReliabilityScore) + '"' + "," + '"' + str(clusterScoreModificationOfTotalScore) + '"' + ","
                + '"' + str(datePublicationAddedToEntrez) + '"' + "," + '"' + str(clusterIdentifier) + '"' + "," + '"' + str(doi) + '"' + "," + '"' + str(issn) + '"' + "," + '"' + str(issue) + '"' + "," + '"' + str(journalTitleISOabbreviation) + '"'  + "," + '"' + str(pages) + '"' + "," + '"' + str(timesCited) + '"' + "," + '"' + str(volume) + '"'
                + "\n")
    count += 1
    print("here:", count)
f.close()

#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####
#code for personArticleGrant table
f = open(outputPath + 'personArticleGrant.csv','w')
f.write("personIdentifier," + "pmid," + "articleGrant," + "grantMatchScore," + "institutionGrant" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'grantEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            grants_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'])
        
            for k in range(grants_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                articleGrant = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['articleGrant']
                grantMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['grantMatchScore']
                institutionGrant = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['institutionGrant']
    
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(articleGrant) + '"' + "," 
                    + str(grantMatchScore)  + "," + '"' + str(institutionGrant) + '"' + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleScopusNonTargetAuthorAffiliation table
f = open(outputPath + 'personArticleScopusNonTargetAuthorAffiliation.csv','w')
f.write("personIdentifier," + "pmid," + "nonTargetAuthorInstitutionLabel," + "nonTargetAuthorInstitutionID," + "nonTargetAuthorInstitutionCount" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            if 'nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']:
                scopusNonTargetAuthorAffiliation_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'])
        
                for k in range(scopusNonTargetAuthorAffiliation_temp):
                    personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                    pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                    nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'][k]
                    #since the nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution field contains more than one featureseparated by comma, and string feature contains comma, we need to disdinguish between this two by the following code
                    count_comma = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.count(',')
                    comma_difference = count_comma - 2
                    if comma_difference != 0:
                        nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.replace(",", ".", comma_difference)
                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleScopusTargetAuthorAffiliation table
f = open(outputPath + 'personArticleScopusTargetAuthorAffiliation.csv','w')
f.write("personIdentifier," + "pmid," + "targetAuthorInstitutionalAffiliationSource," + "scopusTargetAuthorInstitutionalAffiliationIdentity," + "targetAuthorInstitutionalAffiliationArticleScopusLabel,"
        + "targetAuthorInstitutionalAffiliationArticleScopusAffiliationId," + "targetAuthorInstitutionalAffiliationMatchType," + "targetAuthorInstitutionalAffiliationMatchTypeScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'scopusTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            scopusTargetAuthorAffiliation_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'])
        
            for k in range(scopusTargetAuthorAffiliation_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                targetAuthorInstitutionalAffiliationSource = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationSource']
                if 'scopusTargetAuthorInstitutionalAffiliationIdentity' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                    scopusTargetAuthorInstitutionalAffiliationIdentity = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationIdentity']
                else:
                    scopusTargetAuthorInstitutionalAffiliationIdentity = ""
                if 'targetAuthorInstitutionalAffiliationArticleScopusLabel' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                    targetAuthorInstitutionalAffiliationArticleScopusLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusLabel']
                else:
                    targetAuthorInstitutionalAffiliationArticleScopusLabel = ""
                targetAuthorInstitutionalAffiliationArticleScopusAffiliationId = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusAffiliationId']
                targetAuthorInstitutionalAffiliationMatchType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchType']
                targetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchTypeScore']

                f.write(str(personIdentifier) + "," + str(pmid) + "," + str(targetAuthorInstitutionalAffiliationSource) + "," 
                    + '"' + str(scopusTargetAuthorInstitutionalAffiliationIdentity) + '"' + "," + '"' + str(targetAuthorInstitutionalAffiliationArticleScopusLabel) + '"' + "," + str(targetAuthorInstitutionalAffiliationArticleScopusAffiliationId) + "," 
                    + str(targetAuthorInstitutionalAffiliationMatchType) + "," + str(targetAuthorInstitutionalAffiliationMatchTypeScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleDepartment table
f = open(outputPath + 'personArticleDepartment.csv','w')
f.write("personIdentifier," + "pmid," + "identityOrganizationalUnit," + "articleAffiliation," 
        + "organizationalUnitType," + "organizationalUnitMatchingScore," + "organizationalUnitModifier," + "organizationalUnitModifierScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'organizationalUnitEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            organizationalUnit_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'])
        
            for k in range(organizationalUnit_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                identityOrganizationalUnit = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['identityOrganizationalUnit']
                identityOrganizationalUnit = identityOrganizationalUnit.replace('"', '""')
                articleAffiliation = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['articleAffiliation']
                articleAffiliation = articleAffiliation.replace('"', '""')
                organizationalUnitType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitType']
                organizationalUnitMatchingScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitMatchingScore']
                if 'organizationalUnitModifier' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]:
                    organizationalUnitModifier = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifier']
                else:
                    organizationalUnitModifier = ""
                organizationalUnitModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifierScore']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(identityOrganizationalUnit) + '"' + "," 
                    + '"' + str(articleAffiliation) + '"' + "," + str(organizationalUnitType) + "," 
                    + str(organizationalUnitMatchingScore) + "," + str(organizationalUnitModifier) + "," + str(organizationalUnitModifierScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleRelationship table
f = open(outputPath + 'personArticleRelationship.csv','w')
f.write("personIdentifier," + "pmid," + "relationshipNameArticleFirstName," + "relationshipNameArticleLastName," 
        + "relationshipNameIdentityFirstName," + "relationshipNameIdentityLastName," + "relationshipType," + "relationshipMatchType,"
        + "relationshipMatchingScore," + "relationshipVerboseMatchModifierScore," + "relationshipMatchModifierMentor,"
        + "relationshipMatchModifierMentorSeniorAuthor," + "relationshipMatchModifierManager," + "relationshipMatchModifierManagerSeniorAuthor" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'relationshipEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            relationshipPositiveMatch_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'])
        
            for k in range(relationshipPositiveMatch_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                relationshipNameArticle_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['firstName']
                relationshipNameArticle_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['lastName']
                relationshipNameIdentity_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['firstName']
                relationshipNameIdentity_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['lastName']
                relationshipType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipType']
                relationshipMatchType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchType'] 
                relationshipMatchingScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchingScore']
                relationshipVerboseMatchModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipVerboseMatchModifierScore']
                relationshipMatchModifierMentor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentor']
                relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentorSeniorAuthor']
                relationshipMatchModifierManager = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManager']
                relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManagerSeniorAuthor']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                    + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                    + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                    + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                    + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")
    count += 1
    print("here:", count)
f.close()    



#code for person table
f = open(outputPath + 'person.csv','w')
f.write("personIdentifier," + "dateAdded," + "dateUpdated," + "precision," + "recall," + "countSuggestedArticles," + "overallAccuracy," + "mode" + "\n")

count = 0
for i in range(len(items)):
    personIdentifier = items[i]['reCiterFeature']['personIdentifier']
    dateAdded = items[i]['reCiterFeature']['dateAdded']
    dateUpdated = items[i]['reCiterFeature']['dateUpdated']
    precision = items[i]['reCiterFeature']['precision']
    recall = items[i]['reCiterFeature']['recall']
    countSuggestedArticles = items[i]['reCiterFeature']['countSuggestedArticles']
    overallAccuracy = items[i]['reCiterFeature']['overallAccuracy']
    mode = items[i]['reCiterFeature']['mode']
    
    f.write(str(personIdentifier) + "," + str(dateAdded) + "," + str(dateUpdated) + "," 
                + str(precision) + "," + str(recall) + "," 
                + str(countSuggestedArticles) + "," + str(overallAccuracy) + "," + str(mode) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleAuthor table

#record a articles associated number of authors
count_authors_dict = {}
for i in range(len(items)):
    temp = count_articles[i][1]
    for j in range(temp):
        count_authors_dict[str(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid'])] =  len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'])
print(len(count_authors_dict))

f = open(outputPath + 'personArticleAuthor.csv','w')
f.write("personIdentifier," + "pmid," + "authorFirstName," + "authorLastName," + "targetAuthor," + "rank," + "orcid" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    
    for j in range(article_temp):
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        author_temp = count_authors_dict[str(pmid)]
        for k in range(author_temp):
            try:
                if 'firstName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['firstName']
                else:
                    firstName = ""
                lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['lastName']
                targetAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['targetAuthor']
                rank = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['rank']
                if 'orcid' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    orcid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['orcid']
                else:
                    orcid = ""
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(firstName) + '"' + "," + '"' + str(lastName) + '"' + "," + str(targetAuthor) + "," + str(rank) + "," + str(orcid) + "\n")
            except IndexError:
                firstName = ""
                lastName = ""
                targetAuthor = ""
                rank = 0
                orcid = ""
    count += 1
    print("here:", count)
f.close()


DB_HOST = os.getenv('DB_HOST')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

mydb = MySQLdb.connect(host=DB_HOST,
    user=DB_USERNAME,
    passwd=DB_PASSWORD,
    db=DB_NAME)

cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE person")
#Import person table
f = open(outputPath + 'person.csv','r')
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO person(personIdentifier, dateAdded, dateUpdated, `precision`, recall, countSuggestedArticles, overallAccuracy, mode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticleAuthor_s3 table
f = open(outputPath + 'personArticleAuthor.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleAuthor")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleAuthor(personIdentifier, pmid, authorFirstName, authorLastName, targetAuthor, rank, orcid) VALUES(%s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticleRelationship_s3 table
f = open(outputPath + 'personArticleRelationship.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleRelationship")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleRelationship(personIdentifier, pmid, relationshipNameArticleFirstName, relationshipNameArticleLastName, relationshipNameIdentityFirstName, relationshipNameIdentityLastName, relationshipType, relationshipMatchType, relationshipMatchingScore, relationshipVerboseMatchModifierScore, relationshipMatchModifierMentor, relationshipMatchModifierMentorSeniorAuthor, relationshipMatchModifierManager, relationshipMatchModifierManagerSeniorAuthor) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()


#Import personArticleDepartment_s3 table
f = open(outputPath + 'personArticleDepartment.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleDepartment")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleDepartment(personIdentifier, pmid, identityOrganizationalUnit, articleAffiliation, organizationalUnitType, organizationalUnitMatchingScore, organizationalUnitModifier, organizationalUnitModifierScore) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticleScopusTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusTargetAuthorAffiliation.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleScopusTargetAuthorAffiliation")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleScopusTargetAuthorAffiliation(personIdentifier, pmid, targetAuthorInstitutionalAffiliationSource, scopusTargetAuthorInstitutionalAffiliationIdentity, targetAuthorInstitutionalAffiliationArticleScopusLabel, targetAuthorInstitutionalAffiliationArticleScopusAffiliationId, targetAuthorInstitutionalAffiliationMatchType, targetAuthorInstitutionalAffiliationMatchTypeScore) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticleScopusNonTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusNonTargetAuthorAffiliation.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleScopusNonTargetAuthorAffiliation")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleScopusNonTargetAuthorAffiliation(personIdentifier, pmid, nonTargetAuthorInstitutionLabel, nonTargetAuthorInstitutionID, nonTargetAuthorInstitutionCount) VALUES(%s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticleGrant_s3 table
f = open(outputPath + 'personArticleGrant.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticleGrant")
csv_data = csv.reader(f)
#Skip column headers
next(csv_data)
for row in csv_data:
    try:
        cursor.execute("INSERT INTO personArticleGrant(personIdentifier, pmid, articleGrant, grantMatchScore, institutionGrant) VALUES(%s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()

#Import personArticle_s3_mysql table
f = open(outputPath + 'personArticle_mysql.csv','r')
cursor = mydb.cursor()
cursor.execute("TRUNCATE TABLE personArticle")
csv_data = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
#Skip column headers
next(csv_data)
for row in csv_data:
    #cursor.execute('set profiling = 1')
    try:
        cursor.execute("INSERT INTO personArticle(personIdentifier, pmid, pmcid, totalArticleScoreStandardized, totalArticleScoreNonStandardized, userAssertion, publicationDateDisplay, publicationDateStandardized, publicationTypeCanonical, scopusDocID, journalTitleVerbose, articleTitle, feedbackScoreAccepted, feedbackScoreRejected, feedbackScoreNull, articleAuthorNameFirstName, articleAuthorNameLastName, institutionalAuthorNameFirstName, institutionalAuthorNameMiddleName, institutionalAuthorNameLastName, nameMatchFirstScore, nameMatchFirstType, nameMatchMiddleScore, nameMatchMiddleType, nameMatchLastScore, nameMatchLastType, nameMatchModifierScore, nameScoreTotal, emailMatch, emailMatchScore, journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore, relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore, articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore, genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy, personType, personTypeScore, countArticlesRetrieved, articleCountScore, targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore, scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore, totalArticleScoreWithoutClustering, clusterScoreAverage, clusterReliabilityScore, clusterScoreModificationOfTotalScore, datePublicationAddedToEntrez, clusterIdentifier, doi, issn, issue, journalTitleISOabbreviation, pages, timesCited, volume) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
        row)
    except:
        print(cursor._last_executed)
        raise

        #cursor.execute('show profiles')
        #for row in cursor:
            #print(row)        
#cursor.execute('set profiling = 0')
#close the connection to the database.
mydb.commit()
cursor.close()
f.close()