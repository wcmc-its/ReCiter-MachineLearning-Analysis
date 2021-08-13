import json
import time
import os
import csv
from init import pymysql
import MySQLdb

import boto3
import os

def downloadDirectoryFroms3(bucketName,remoteDirectoryName):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucketName)
    number = 0
    for object in bucket.objects.filter(Prefix = remoteDirectoryName):
        number = number + 1
        #if number == 500:
        #    break
        print(object)
        if not os.path.exists(os.path.dirname(object.key)):
            os.makedirs(os.path.dirname(object.key))
        bucket.download_file(object.key,object.key)

outputPath = '/usr/src/app/ReCiter/'

#get all filename(i.e. personIdentifier) in the folder
originalDataPath = '/usr/src/app/AnalysisOutput/' #need to modify based on your local directory

downloadDirectoryFroms3(os.getenv('S3_BUCKET_NAME'), 'AnalysisOutput')

person_list = []
for filename in os.listdir(originalDataPath):
    person_list.append(filename)
# person_list.remove('.DS_Store')
person_list.sort()
print(len(person_list))


#use the directory to read files in 
items = []
for item in person_list:
    #record time
    start = time.time()
    for line in open(originalDataPath + '{}'.format(item), 'r'): 
        items.append(json.loads(line))
    print('execution time:', time.time() - start)
print(len(items))




#code for personArticle_s3 table
#open a csv file
f = open(outputPath + 'personArticle_s3_mysql.csv','w')

#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']

        totalArticleScoreStandardized = items[i]['reCiterArticleFeatures'][j]['totalArticleScoreStandardized']
        totalArticleScoreNonStandardized = items[i]['reCiterArticleFeatures'][j]['totalArticleScoreNonStandardized']
        userAssertion = items[i]['reCiterArticleFeatures'][j]['userAssertion']
        publicationDateStandardized = items[i]['reCiterArticleFeatures'][j]['publicationDateStandardized']
        if 'publicationTypeCanonical' in items[i]['reCiterArticleFeatures'][j]['publicationType']:
            publicationTypeCanonical = items[i]['reCiterArticleFeatures'][j]['publicationType']['publicationTypeCanonical']
        else:
            publicationTypeCanonical = ""
        # example1: when you get key error, check whether the key exist in dynamodb or not
        if 'scopusDocID' in items[i]['reCiterArticleFeatures'][j]:
            scopusDocID = items[i]['reCiterArticleFeatures'][j]['scopusDocID']
        else:
            scopusDocID = ""

        if 'pmcid' in items[i]['reCiterArticleFeatures'][j]:
            pmcid = items[i]['reCiterArticleFeatures'][j]['pmcid']
        else:
            pmcid = ""

        journalTitleVerbose = items[i]['reCiterArticleFeatures'][j]['journalTitleVerbose']
        journalTitleVerbose = journalTitleVerbose.replace('"', '""')
        if 'articleTitle' in items[i]['reCiterArticleFeatures'][j]:
            articleTitle = items[i]['reCiterArticleFeatures'][j]['articleTitle']
            articleTitle = articleTitle.replace('"', '""')
        else:
            articleTitle = ""

        if 'reCiterArticleAuthorFeatures' not in items[i]['reCiterArticleFeatures'][j]:
            largeGroupAuthorship = True
        else:
            largeGroupAuthorship = False
        if 'evidence' in items[i]['reCiterArticleFeatures'][j]:
            if 'acceptedRejectedEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'feedbackScoreAccepted' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreAccepted = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreAccepted']
                else: 
                    feedbackScoreAccepted = 0
                if 'feedbackScoreRejected' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreRejected = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreRejected']
                else: 
                    feedbackScoreRejected = 0
                if 'feedbackScoreNull' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreNull = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreNull']
                else: 
                    feedbackScoreNull = 0
            if 'authorNameEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'articleAuthorName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    if 'firstName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']: 
                        articleAuthorName_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['firstName']
                    else:
                        articleAuthorName_firstName = ""
                    articleAuthorName_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['lastName']
                else:
                    articleAuthorName_firstName, articleAuthorName_lastName = "", ""
                institutionalAuthorName_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['firstName']
                if 'middleName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']:
                    institutionalAuthorName_middleName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['middleName']
                else:
                    institutionalAuthorName_middleName = ""
                institutionalAuthorName_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['lastName']
                nameMatchFirstScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstScore']
                if 'nameMatchFirstType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchFirstType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstType']
                nameMatchMiddleScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleScore']
                if 'nameMatchMiddleType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchMiddleType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleType']
                nameMatchLastScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastScore']
                if 'nameMatchLastType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchLastType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastType']
                nameMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchModifierScore']
                nameScoreTotal = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameScoreTotal']

            if 'emailEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                emailMatch = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatch']
                if 'false' in emailMatch:
                    emailMatch = ""
                emailMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatchScore']
            else:
                emailMatch, emailMatchScore = "", 0
            
            if 'journalCategoryEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                journalSubfieldScienceMetrixLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixLabel']
                journalSubfieldScienceMetrixLabel = journalSubfieldScienceMetrixLabel.replace('"', '""')
                journalSubfieldScienceMetrixID = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixID']
                journalSubfieldDepartment = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldDepartment']
                journalSubfieldDepartment = journalSubfieldDepartment.replace('"', '""')
                journalSubfieldScore = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScore']
            else:
                journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore = "", "", "", 0
            
            if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'relationshipEvidenceTotalScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                    relationshipEvidenceTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipEvidenceTotalScore']
                else:
                    relationshipEvidenceTotalScore = 0
                if 'relationshipNegativeMatch' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                    relationshipMinimumTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipMinimumTotalScore']
                    relationshipNonMatchCount = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchCount']
                    relationshipNonMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchScore']
                else:
                    relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0
            else:
                relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0, 0
            
            if 'educationYearEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'articleYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    articleYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['articleYear']
                else:
                    articleYear = 0
                if 'identityBachelorYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    identityBachelorYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityBachelorYear']
                else:
                    identityBachelorYear = ""
                if 'discrepancyDegreeYearBachelor' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearBachelor = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelor']
                else:
                    discrepancyDegreeYearBachelor = 0
                if 'discrepancyDegreeYearBachelorScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearBachelorScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelorScore']
                else:
                    discrepancyDegreeYearBachelorScore = 0
                if 'identityDoctoralYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    identityDoctoralYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityDoctoralYear']
                else:
                    identityDoctoralYear = ""
                if 'discrepancyDegreeYearDoctoral' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearDoctoral = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoral']
                else:
                    discrepancyDegreeYearDoctoral = 0
                if 'discrepancyDegreeYearDoctoralScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearDoctoralScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoralScore']
                else:
                    discrepancyDegreeYearDoctoralScore = 0
            else:
                articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore = 0, "", 0, 0, "", 0, 0
            
            if 'genderEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                genderScoreArticle = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreArticle']
                genderScoreIdentity = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentity']
                genderScoreIdentityArticleDiscrepancy = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentityArticleDiscrepancy']
            else:
                genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy = 0, 0, 0
            
            if 'personTypeEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                personType = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personType']
                personTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personTypeScore']
            else:
                personType, personTypeScore = "", 0
            
            if 'articleCountEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                countArticlesRetrieved = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['countArticlesRetrieved']
                articleCountScore = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['articleCountScore']
            else:
                countArticlesRetrieved,  articleCountScore= 0,0
            
            if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'pubmedTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationArticlePubmedLabel']
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel = targetAuthorInstitutionalAffiliationArticlePubmedLabel.replace('"', '""')
                    pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationMatchTypeScore']
                else:
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = "", 0
            
            if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                    scopusNonTargetAuthorInstitutionalAffiliationSource = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationSource']
                    scopusNonTargetAuthorInstitutionalAffiliationScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationScore']
                else:
                    scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore= "", 0

            if 'averageClusteringEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                totalArticleScoreWithoutClustering = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['totalArticleScoreWithoutClustering']
                clusterScoreAverage = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreAverage']
                clusterReliabilityScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterReliabilityScore']
                clusterScoreModificationOfTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreModificationOfTotalScore']
                if 'clusterIdentifier' in items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']:
                    clusterIdentifier = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterIdentifier']
                else :
                    clusterIdentifier = 0

        if 'publicationDateDisplay' in items[i]['reCiterArticleFeatures'][j]:
            publicationDateDisplay = items[i]['reCiterArticleFeatures'][j]['publicationDateDisplay']
        else:
            publicationDateDisplay = ""

        if 'datePublicationAddedToEntrez' in items[i]['reCiterArticleFeatures'][j]:
            datePublicationAddedToEntrez = items[i]['reCiterArticleFeatures'][j]['datePublicationAddedToEntrez']
        else:
            datePublicationAddedToEntrez = ""

        if 'doi' in items[i]['reCiterArticleFeatures'][j]:
            doi = items[i]['reCiterArticleFeatures'][j]['doi']
        else: 
            doi = ""
        #print(items[i]['reCiterArticleFeatures'][j])
        if 'issn' in items[i]['reCiterArticleFeatures'][j]:
            issn_temp = len(items[i]['reCiterArticleFeatures'][j]['issn'])
            for k in range(issn_temp):
                issntype = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issntype']
                if issntype == 'Linking':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Print':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Electronic':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
        else:
            issn = ""

        if 'issue' in items[i]['reCiterArticleFeatures'][j]:
            issue = items[i]['reCiterArticleFeatures'][j]['issue']
        else: 
            issue = ""
        if 'journalTitleISOabbreviation' in items[i]['reCiterArticleFeatures'][j]:
            journalTitleISOabbreviation = items[i]['reCiterArticleFeatures'][j]['journalTitleISOabbreviation']
            journalTitleISOabbreviation = journalTitleISOabbreviation.replace('"', '""')
        else:
            journalTitleISOabbreviation = ""
        if 'pages' in items[i]['reCiterArticleFeatures'][j]:
            pages = items[i]['reCiterArticleFeatures'][j]['pages']
        else:
            pages = ""
        if 'timesCited' in items[i]['reCiterArticleFeatures'][j]:
            timesCited = items[i]['reCiterArticleFeatures'][j]['timesCited']
        else: 
            timesCited = 0
        if 'volume' in items[i]['reCiterArticleFeatures'][j]:
            volume = items[i]['reCiterArticleFeatures'][j]['volume']
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
#code for personArticleGrant_s3 table
f = open(outputPath + 'personArticleGrant_s3.csv','w')

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        if 'grantEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            grants_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'])
        
            for k in range(grants_temp):
                personIdentifier = items[i]['personIdentifier']
                pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                articleGrant = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['articleGrant']
                grantMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['grantMatchScore']
                institutionGrant = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['institutionGrant']
    
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(articleGrant) + '"' + "," 
                    + str(grantMatchScore)  + "," + '"' + str(institutionGrant) + '"' + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleScopusNonTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusNonTargetAuthorAffiliation_s3.csv','w')

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                if 'nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']:
                    scopusNonTargetAuthorAffiliation_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'])
            
                    for k in range(scopusNonTargetAuthorAffiliation_temp):
                        personIdentifier = items[i]['personIdentifier']
                        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                        nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'][k]
                        #since the nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution field contains more than one featureseparated by comma, and string feature contains comma, we need to disdinguish between this two by the following code
                        count_comma = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.count(',')
                        comma_difference = count_comma - 2
                        if comma_difference != 0:
                            nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.replace(",", ".", comma_difference)
                        f.write(str(personIdentifier) + "," + str(pmid) + "," + str(nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution) + "\n")
    count += 1
    print("here:", count)
f.close()


#code for personArticleScopusTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusTargetAuthorAffiliation_s3.csv','w')

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'scopusTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                scopusTargetAuthorAffiliation_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'])
            
                for k in range(scopusTargetAuthorAffiliation_temp):
                    personIdentifier = items[i]['personIdentifier']
                    pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                    targetAuthorInstitutionalAffiliationSource = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationSource']
                    if 'scopusTargetAuthorInstitutionalAffiliationIdentity' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                        scopusTargetAuthorInstitutionalAffiliationIdentity = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationIdentity']
                    else:
                        scopusTargetAuthorInstitutionalAffiliationIdentity = ""
                    if 'targetAuthorInstitutionalAffiliationArticleScopusLabel' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                        targetAuthorInstitutionalAffiliationArticleScopusLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusLabel']
                    else:
                        targetAuthorInstitutionalAffiliationArticleScopusLabel = ""
                    targetAuthorInstitutionalAffiliationArticleScopusAffiliationId = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusAffiliationId']
                    targetAuthorInstitutionalAffiliationMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchType']
                    targetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchTypeScore']

                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(targetAuthorInstitutionalAffiliationSource) + "," 
                        + '"' + str(scopusTargetAuthorInstitutionalAffiliationIdentity) + '"' + "," + '"' + str(targetAuthorInstitutionalAffiliationArticleScopusLabel) + '"' + "," + str(targetAuthorInstitutionalAffiliationArticleScopusAffiliationId) + "," 
                        + str(targetAuthorInstitutionalAffiliationMatchType) + "," + str(targetAuthorInstitutionalAffiliationMatchTypeScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleDepartment_s3 table 
f = open(outputPath + 'personArticleDepartment_s3.csv','w')

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        if 'organizationalUnitEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            organizationalUnit_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'])
        
            for k in range(organizationalUnit_temp):
                personIdentifier = items[i]['personIdentifier']
                pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                identityOrganizationalUnit = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['identityOrganizationalUnit']
                identityOrganizationalUnit = identityOrganizationalUnit.replace('"', '""')
                articleAffiliation = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['articleAffiliation']
                articleAffiliation = articleAffiliation.replace('"', '""')
                organizationalUnitType = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitType']
                organizationalUnitMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitMatchingScore']
                if 'organizationalUnitModifier' in items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]:
                    organizationalUnitModifier = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifier']
                else:
                    organizationalUnitModifier = ""
                organizationalUnitModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifierScore']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(identityOrganizationalUnit) + '"' + "," 
                    + '"' + str(articleAffiliation) + '"' + "," + str(organizationalUnitType) + "," 
                    + str(organizationalUnitMatchingScore) + "," + str(organizationalUnitModifier) + "," + str(organizationalUnitModifierScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for personArticleRelationship_s3 table
f = open(outputPath + 'personArticleRelationship_s3.csv','w')

#capture misspelling key in the content
misspelling_list = []
count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            #the nested key structure is different for every file, so we need to consider two conditions here
            if 'relationshipEvidenceTotalScore' not in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipPositiveMatch_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'])
                for k in range(relationshipPositiveMatch_temp):
                    relationshipNameArticle_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameArticle']['firstName']
                    relationshipNameArticle_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameArticle']['lastName']
                    
                    if 'relationshipNameIdenity' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]:
                        misspelling_list.append((personIdentifier, pmid))
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdenity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdenity']['lastName']
                    else:
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdentity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdentity']['lastName']
                    
                    if 'relationshipType' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]:
                        relationshipType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipType']
                    else:
                        relationshipType = ""
                    relationshipMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchType'] 
                    relationshipMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchingScore']
                    relationshipVerboseMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipVerboseMatchModifierScore']
                    relationshipMatchModifierMentor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierMentor']
                    relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierMentorSeniorAuthor']
                    relationshipMatchModifierManager = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierManager']
                    relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierManagerSeniorAuthor']

                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                        + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                        + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                        + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                        + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")                    
            
            if 'relationshipPositiveMatch' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipPositiveMatch_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'])
                for k in range(relationshipPositiveMatch_temp):
                    relationshipNameArticle_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['firstName']
                    relationshipNameArticle_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['lastName']
                    
                    if 'relationshipNameIdenity' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]:
                        misspelling_list.append((personIdentifier, pmid))
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdenity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdenity']['lastName']
                    else:
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['lastName']
                    if 'relationshipType' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]:
                        relationshipType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipType']
                    else:
                        relationshipType = ""
                    relationshipMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchType'] 
                    relationshipMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchingScore']
                    relationshipVerboseMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipVerboseMatchModifierScore']
                    relationshipMatchModifierMentor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentor']
                    relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentorSeniorAuthor']
                    relationshipMatchModifierManager = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManager']
                    relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManagerSeniorAuthor']
                    
                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                        + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                        + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                        + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                        + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")
    count += 1
    print("here:", count)
f.close()
print(misspelling_list)


#code for personArticleAuthor_s3 table
f = open(outputPath + 'personArticleAuthor_s3.csv','w')
#some article is group authorship, so there is no record for authors in the file, here we use a list to record this
no_reCiterArticleAuthorFeatures_list =[]
count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'reCiterArticleAuthorFeatures' in items[i]['reCiterArticleFeatures'][j]:
            author_temp = len(items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'])
            for k in range(author_temp):
                if 'firstName' in items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    firstName = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['firstName']
                else:
                    firstName = ""
                lastName = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['lastName']                
                targetAuthor = int(items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['targetAuthor'])                
                rank = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['rank']
                if 'orcid' in items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    orcid = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['orcid']
                else:
                    orcid = ""
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(firstName) + '"' + "," + '"' + str(lastName) + '"' + "," + str(targetAuthor) + "," + str(rank) + "," + str(orcid) + "\n")
        else:
            no_reCiterArticleAuthorFeatures_list.append((personIdentifier, pmid))
    count += 1
    print("here:", count)
f.close()
print(no_reCiterArticleAuthorFeatures_list)



#code for person table
f = open(outputPath + 'person_s3.csv','w')

count = 0
for i in range(len(items)):
    personIdentifier = items[i]['personIdentifier']
    dateAdded = items[i]['dateAdded']
    dateUpdated = items[i]['dateUpdated']
    precision = items[i]['precision']
    recall = items[i]['recall']
    countSuggestedArticles = items[i]['countSuggestedArticles']
    overallAccuracy = items[i]['overallAccuracy']
    mode = items[i]['mode']

    f.write(str(personIdentifier) + "," + str(dateAdded) + "," + str(dateUpdated) + "," 
                + str(precision) + "," + str(recall) + "," 
                + str(countSuggestedArticles) + "," + str(overallAccuracy) + "," + str(mode) + "\n")
    count += 1
    print("here:", count)
f.close()

#code for personArticleKeyword_s3 table
#open a csv file
f = open(outputPath + 'personArticleKeyword_s3.csv','w')

#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'articleKeywords' in items[i]['reCiterArticleFeatures'][j]:
            keywords_temp = len(items[i]['reCiterArticleFeatures'][j]['articleKeywords'])
            for k in range(keywords_temp):
                if 'keyword' in items[i]['reCiterArticleFeatures'][j]['articleKeywords'][k]:
                    keyword = items[i]['reCiterArticleFeatures'][j]['articleKeywords'][k]['keyword']
                else: 
                    keyword = ""
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(keyword) + '"' + "\n")
            
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
cursor.execute('SET autocommit = 0')
mydb.commit()

#Import person table
f = open(outputPath + 'person_s3.csv','r')
csv_data = csv.reader(f)
person = []
for row in csv_data:
    person.append(tuple(row))
    
cursor.executemany("INSERT INTO person(personIdentifier, dateAdded, dateUpdated, `precision`, recall, countSuggestedArticles, overallAccuracy, mode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", person)
mydb.commit()
f.close()

#Import personArticleAuthor_s3 table
f = open(outputPath + 'personArticleAuthor_s3.csv','r')
csv_data = csv.reader(f)
personArticleAuthor = []
for row in csv_data:
    personArticleAuthor.append(tuple(row))

cursor.executemany("INSERT INTO personArticleAuthor(personIdentifier, pmid, authorFirstName, authorLastName, targetAuthor, rank, orcid) VALUES(%s, %s, %s, %s, %s, %s, %s)", personArticleAuthor)
mydb.commit()
f.close()

#Import personArticleRelationship_s3 table
f = open(outputPath + 'personArticleRelationship_s3.csv','r')
csv_data = csv.reader(f)
personArticleRelationship = []
for row in csv_data:
    personArticleRelationship.append(tuple(row))
    
cursor.executemany("INSERT INTO personArticleRelationship(personIdentifier, pmid, relationshipNameArticleFirstName, relationshipNameArticleLastName, relationshipNameIdentityFirstName, relationshipNameIdentityLastName, relationshipType, relationshipMatchType, relationshipMatchingScore, relationshipVerboseMatchModifierScore, relationshipMatchModifierMentor, relationshipMatchModifierMentorSeniorAuthor, relationshipMatchModifierManager, relationshipMatchModifierManagerSeniorAuthor) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", personArticleRelationship)
mydb.commit()
f.close()


#Import personArticleDepartment_s3 table
f = open(outputPath + 'personArticleDepartment_s3.csv','r')
csv_data = csv.reader(f)
personArticleDepartment = []
for row in csv_data:
    personArticleDepartment.append(tuple(row))

cursor.executemany("INSERT INTO personArticleDepartment(personIdentifier, pmid, identityOrganizationalUnit, articleAffiliation, organizationalUnitType, organizationalUnitMatchingScore, organizationalUnitModifier, organizationalUnitModifierScore) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", personArticleDepartment)
mydb.commit()
f.close()

#Import personArticleScopusTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusTargetAuthorAffiliation_s3.csv','r')
csv_data = csv.reader(f)
personArticleScopusTargetAuthorAffiliation = []
for row in csv_data:
    personArticleScopusTargetAuthorAffiliation.append(tuple(row))

cursor.executemany("INSERT INTO personArticleScopusTargetAuthorAffiliation(personIdentifier, pmid, targetAuthorInstitutionalAffiliationSource, scopusTargetAuthorInstitutionalAffiliationIdentity, targetAuthorInstitutionalAffiliationArticleScopusLabel, targetAuthorInstitutionalAffiliationArticleScopusAffiliationId, targetAuthorInstitutionalAffiliationMatchType, targetAuthorInstitutionalAffiliationMatchTypeScore) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", personArticleScopusTargetAuthorAffiliation)
mydb.commit()
f.close()

#Import personArticleScopusNonTargetAuthorAffiliation_s3 table
f = open(outputPath + 'personArticleScopusNonTargetAuthorAffiliation_s3.csv','r')
csv_data = csv.reader(f)
personArticleScopusNonTargetAuthorAffiliation = []
for row in csv_data:
    personArticleScopusNonTargetAuthorAffiliation.append(tuple(row))

cursor.executemany("INSERT INTO personArticleScopusNonTargetAuthorAffiliation(personIdentifier, pmid, nonTargetAuthorInstitutionLabel, nonTargetAuthorInstitutionID, nonTargetAuthorInstitutionCount) VALUES(%s, %s, %s, %s, %s)", personArticleScopusNonTargetAuthorAffiliation)
mydb.commit()
f.close()

#Import personArticleGrant_s3 table
f = open(outputPath + 'personArticleGrant_s3.csv','r')
csv_data = csv.reader(f)
personArticleGrant = []
for row in csv_data:
    personArticleGrant.append(tuple(row))

cursor.executemany("INSERT INTO personArticleGrant(personIdentifier, pmid, articleGrant, grantMatchScore, institutionGrant) VALUES(%s, %s, %s, %s, %s)", personArticleGrant)
mydb.commit()
f.close()

#Import personArticleKeyword table
f = open(outputPath + 'personArticleKeyword_s3.csv','r')
csv_data = csv.reader(f)
personArticleKeyword = []
for row in csv_data:
    personArticleKeyword.append(tuple(row))

cursor.executemany("INSERT INTO personArticleKeyword(personIdentifier, pmid, keyword) VALUES(%s, %s, %s)", personArticleKeyword)
mydb.commit()
f.close()

#Import personArticle_s3_mysql table
f = open(outputPath + 'personArticle_s3_mysql.csv','r')
csv_data = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
personArticle = []
for row in csv_data:
    personArticle.append(tuple(row))

cursor.executemany("INSERT INTO personArticle(personIdentifier, pmid, pmcid, totalArticleScoreStandardized, totalArticleScoreNonStandardized, userAssertion, publicationDateDisplay, publicationDateStandardized, publicationTypeCanonical, scopusDocID, journalTitleVerbose, articleTitle, feedbackScoreAccepted, feedbackScoreRejected, feedbackScoreNull, articleAuthorNameFirstName, articleAuthorNameLastName, institutionalAuthorNameFirstName, institutionalAuthorNameMiddleName, institutionalAuthorNameLastName, nameMatchFirstScore, nameMatchFirstType, nameMatchMiddleScore, nameMatchMiddleType, nameMatchLastScore, nameMatchLastType, nameMatchModifierScore, nameScoreTotal, emailMatch, emailMatchScore, journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore, relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore, articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore, genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy, personType, personTypeScore, countArticlesRetrieved, articleCountScore, targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore, scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore, totalArticleScoreWithoutClustering, clusterScoreAverage, clusterReliabilityScore, clusterScoreModificationOfTotalScore, datePublicationAddedToEntrez, clusterIdentifier, doi, issn, issue, journalTitleISOabbreviation, pages, timesCited, volume) VALUES(%s, %s, NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''), NULLIF(%s,''))",
 personArticle)

#close the connection to the database.
mydb.commit()
cursor.close()
f.close() 