import json
import time
import os

#get all filename(i.e. personIdentifier.json) in the folder
originalDataPath = '/Users/zhuzm1996/Desktop/reciterTest/' #need to modify based on your local directory

person_list = []
for filename in os.listdir(originalDataPath):
    person_list.append(filename)
person_list.remove('.DS_Store')
person_list.sort()
print(len(person_list))


#use the directory to read files in
items = []
for item in person_list:
    start = time.time()
    for line in open(originalDataPath + '{}'.format(item), 'r'):
        items.append(json.loads(line))
    print('execution time:', time.time() - start)
print(len(items))


outputPath = '/Users/zhuzm1996/Desktop/ReCiter/'

#code for personArticle_ML tabel
#open a csv file
f = open(outputPath + 'personArticle_ML_mysql.csv','w')
#write column names into file
f.write("personIdentifier," + "pmid," + "totalArticleScoreStandardized," + "totalArticleScoreNonStandardized," 
        + "userAssertion," + "publicationDateStandardized," + "publicationTypeCanonical," + "publicationAbstract,"
        + "scopusDocID," + "journalTitleVerbose," + "articleTitle," + "largeGroupAuthorship," 
        + "articleAuthorNameFirstName," + "articleAuthorNameLastName," + "institutionalAuthorNameFirstName," + "institutionalAuthorNameMiddleName," + "institutionalAuthorNameLastName,"
        + "nameMatchFirstScore," + "nameMatchFirstType," + "nameMatchMiddleScore," + "nameMatchMiddleType," + "nameMatchLastScore," + "nameMatchLastType," + "nameMatchModifierScore," + "nameScoreTotal,"
        + "emailMatch," + "emailMatchScore," + "journalSubfieldScienceMetrixLabel," + "journalSubfieldScienceMetrixID," + "journalSubfieldDepartment," + "journalSubfieldScore,"
        + "relationshipEvidenceTotalScore," + "relationshipMinimumTotalScore," + "relationshipNonMatchCount," + "relationshipNonMatchScore,"
        + "articleYear," + "identityBachelorYear," + "discrepancyDegreeYearBachelor," + "discrepancyDegreeYearBachelorScore," + "identityDoctoralYear,"
        + "discrepancyDegreeYearDoctoral," + "discrepancyDegreeYearDoctoralScore," + "genderScoreArticle," + "genderScoreIdentity," + "genderScoreIdentityArticleDiscrepancy,"
        + "personType," + "personTypeScore," + "countArticlesRetrieved," + "articleCountScore," 
        + "targetAuthorInstitutionalAffiliationArticlePubmedLabel," + "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore," + "scopusNonTargetAuthorInstitutionalAffiliationSource," + "scopusNonTargetAuthorInstitutionalAffiliationScore,"
        + "totalArticleScoreWithoutClustering," + "clusterScoreAverage," + "clusterReliabilityScore," + "clusterScoreModificationOfTotalScore," 
        + "datePublicationAddedToEntrez," + "doi," + "issn," + "issue," + "journalTitleISOabbreviation," + "pages," + "timesCited," + "volume"
        + "\n")

#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 
for i in range(len(items)):
    personIdentifier = items[i]['personIdentifier']
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
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
        if 'publicationAbstract' in items[i]['reCiterArticleFeatures'][j]:
            publicationAbstract = items[i]['reCiterArticleFeatures'][j]['publicationAbstract']
            #if the value is a string, it may contain ", we need to replace it with ""
            publicationAbstract = publicationAbstract.replace('"', '""')
        else:
            publicationAbstract = ""
        if 'scopusDocID' in items[i]['reCiterArticleFeatures'][j]:
            scopusDocID = items[i]['reCiterArticleFeatures'][j]['scopusDocID']
        else:
            scopusDocID = ""
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
        nameMatchFirstType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstType']
        nameMatchMiddleScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleScore']
        nameMatchMiddleType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleType']
        nameMatchLastScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastScore']
        nameMatchLastType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastType']
        nameMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchModifierScore']
        nameScoreTotal = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameScoreTotal']

        if 'emailEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            emailMatch = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatch']
            if 'false' in emailMatch:
                emailMatch = ""
            emailMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatchScore']
        else:
            emailMatch, emailMatchScore = "", ""
        
        if 'journalCategoryEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            journalSubfieldScienceMetrixLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixLabel']
            journalSubfieldScienceMetrixLabel = journalSubfieldScienceMetrixLabel.replace('"', '""')
            journalSubfieldScienceMetrixID = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixID']
            journalSubfieldDepartment = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldDepartment']
            journalSubfieldDepartment = journalSubfieldDepartment.replace('"', '""')
            journalSubfieldScore = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScore']
        else:
            journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore = "", "", "", ""
        
        if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'relationshipEvidenceTotalScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipEvidenceTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipEvidenceTotalScore']
            else:
                relationshipEvidenceTotalScore = ""
            if 'relationshipNegativeMatch' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipMinimumTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipMinimumTotalScore']
                relationshipNonMatchCount = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchCount']
                relationshipNonMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchScore']
            else:
                relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = "", "", ""
        else:
            relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = "", "", "", ""
        
        if 'educationYearEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'articleYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                articleYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['articleYear']
            else:
                articleYear = ""
            if 'identityBachelorYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityBachelorYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityBachelorYear']
            else:
                identityBachelorYear = ""
            if 'discrepancyDegreeYearBachelor' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelor = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelor']
            else:
                discrepancyDegreeYearBachelor = ""
            if 'discrepancyDegreeYearBachelorScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelorScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelorScore']
            else:
                discrepancyDegreeYearBachelorScore = ""
            if 'identityDoctoralYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityDoctoralYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityDoctoralYear']
            else:
                identityDoctoralYear = ""
            if 'discrepancyDegreeYearDoctoral' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoral = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoral']
            else:
                discrepancyDegreeYearDoctoral = ""
            if 'discrepancyDegreeYearDoctoralScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoralScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoralScore']
            else:
                discrepancyDegreeYearDoctoralScore = ""
        else:
            articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore = "", "", "", "", "", "", ""
        
        if 'genderEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            genderScoreArticle = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreArticle']
            genderScoreIdentity = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentity']
            genderScoreIdentityArticleDiscrepancy = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentityArticleDiscrepancy']
        else:
            genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy = "", "", ""
        
        if 'personTypeEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            personType = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personType']
            personTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personTypeScore']
        else:
            personType, personTypeScore = "", ""
        
        countArticlesRetrieved = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['countArticlesRetrieved']
        articleCountScore = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['articleCountScore']
        
        if 'pubmedTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationArticlePubmedLabel']
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = targetAuthorInstitutionalAffiliationArticlePubmedLabel.replace('"', '""')
            pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationMatchTypeScore']
        else:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = "", ""
        
        if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            scopusNonTargetAuthorInstitutionalAffiliationSource = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationSource']
            scopusNonTargetAuthorInstitutionalAffiliationScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationScore']
        else:
            scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore= "", ""
        
        totalArticleScoreWithoutClustering = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['totalArticleScoreWithoutClustering']
        clusterScoreAverage = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreAverage']
        clusterReliabilityScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterReliabilityScore']
        clusterScoreModificationOfTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreModificationOfTotalScore']

        if 'datePublicationAddedToEntrez' in items[i]['reCiterArticleFeatures'][j]:
            datePublicationAddedToEntrez = items[i]['reCiterArticleFeatures'][j]['datePublicationAddedToEntrez']
        else:
            datePublicationAddedToEntrez = ""
        if 'doi' in items[i]['reCiterArticleFeatures'][j]:
            doi = items[i]['reCiterArticleFeatures'][j]['doi']
        else: 
            doi = ""

        if 'issn' in items[i]['reCiterArticleFeatures'][j]:
            issn_temp = len(items[i]['reCiterArticleFeatures'][j]['issn'])
            for k in range(issn_temp):
                issntype = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issntype']
                if issntype == 'Linking':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
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
            timesCited = ""
        if 'volume' in items[i]['reCiterArticleFeatures'][j]:
            volume = items[i]['reCiterArticleFeatures'][j]['volume']
        else:
            volume = ""
        
        #write all extracted features into csv file
        #some string value may contain a comma, in this case, we need to double quote the output value, for example, '"' + str(journalSubfieldScienceMetrixLabel) + '"'
        f.write(str(personIdentifier) + "," + str(pmid) + "," + str(totalArticleScoreStandardized) + "," 
                + str(totalArticleScoreNonStandardized) + "," + str(userAssertion) + "," 
                + str(publicationDateStandardized) + "," + str(publicationTypeCanonical) + "," + '"' + str(publicationAbstract) + '"' + ","
                + str(scopusDocID) + ","  + '"' + str(journalTitleVerbose) + '"' + "," + '"' + str(articleTitle) + '"' + "," + str(largeGroupAuthorship) + "," 
                + str(articleAuthorName_firstName) + "," + str(articleAuthorName_lastName) + "," + str(institutionalAuthorName_firstName) + "," + str(institutionalAuthorName_middleName) + "," + '"' + str(institutionalAuthorName_lastName) + '"' + ","
                + str(nameMatchFirstScore) + "," + str(nameMatchFirstType) + "," + str(nameMatchMiddleScore) + "," + str(nameMatchMiddleType) + ","
                + str(nameMatchLastScore) + "," + str(nameMatchLastType) + "," + str(nameMatchModifierScore) + "," + str(nameScoreTotal) + ","
                + str(emailMatch) + "," + str(emailMatchScore) + "," 
                + '"' + str(journalSubfieldScienceMetrixLabel) + '"' + "," + str(journalSubfieldScienceMetrixID) + "," + '"' + str(journalSubfieldDepartment) + '"' + "," + str(journalSubfieldScore) + "," 
                + str(relationshipEvidenceTotalScore) + "," + str(relationshipMinimumTotalScore) + "," + str(relationshipNonMatchCount) + "," + str(relationshipNonMatchScore) + ","
                + str(articleYear) + "," + str(identityBachelorYear) + "," + str(discrepancyDegreeYearBachelor) + "," + str(discrepancyDegreeYearBachelorScore) + ","
                + str(identityDoctoralYear) + "," + str(discrepancyDegreeYearDoctoral) + "," + str(discrepancyDegreeYearDoctoralScore) + "," 
                + str(genderScoreArticle) + "," + str(genderScoreIdentity) + "," + str(genderScoreIdentityArticleDiscrepancy) + "," 
                + str(personType) + "," + str(personTypeScore) + ","
                + str(countArticlesRetrieved) + "," + str(articleCountScore) + ","
                + '"' + str(targetAuthorInstitutionalAffiliationArticlePubmedLabel) + '"' + "," + str(pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore) + "," + str(scopusNonTargetAuthorInstitutionalAffiliationSource) + "," + str(scopusNonTargetAuthorInstitutionalAffiliationScore) + ","
                + str(totalArticleScoreWithoutClustering) + "," + str(clusterScoreAverage) + "," + str(clusterReliabilityScore) + "," + str(clusterScoreModificationOfTotalScore) + ","
                + str(datePublicationAddedToEntrez) + "," + str(doi) + "," + str(issn) + "," + '"' + str(issue) + '"' + "," + '"' + str(journalTitleISOabbreviation) + '"'  + "," + '"' + str(pages) + '"' + "," + str(timesCited) + "," + '"' + str(volume) + '"'
                + "\n")
    count += 1
    print("here:", count)
f.close()


#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####
#code for personArticleGrant_ML table
f = open(outputPath + 'personArticleGrant_ML.csv','w')
f.write("personIdentifier," + "pmid," + "articleGrant," + "grantMatchScore," + "institutionGrant" + "\n")

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


#code for personArticleScopusTargetAuthorAffiliation_ML table
f = open(outputPath + 'personArticleScopusTargetAuthorAffiliation_ML.csv','w')
f.write("personIdentifier," + "pmid," + "targetAuthorInstitutionalAffiliationSource," + "scopusTargetAuthorInstitutionalAffiliationIdentity," + "targetAuthorInstitutionalAffiliationArticleScopusLabel,"
        + "targetAuthorInstitutionalAffiliationArticleScopusAffiliationId," + "targetAuthorInstitutionalAffiliationMatchType," + "targetAuthorInstitutionalAffiliationMatchTypeScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
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


#code for personArticleDepartment_ML table 
f = open(outputPath + 'personArticleDepartment_ML.csv','w')
f.write("personIdentifier," + "pmid," + "identityOrganizationalUnit," + "articleAffiliation," 
        + "organizationalUnitType," + "organizationalUnitMatchingScore," + "organizationalUnitModifier," + "organizationalUnitModifierScore" + "\n")

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


#code for personArticleRelationship_ML table
f = open(outputPath + 'personArticleRelationship_ML.csv','w')
f.write("personIdentifier," + "pmid," + "relationshipNameArticleFirstName," + "relationshipNameArticleLastName," 
        + "relationshipNameIdentityFirstName," + "relationshipNameIdentityLastName," + "relationshipType," + "relationshipMatchType,"
        + "relationshipMatchingScore," + "relationshipVerboseMatchModifierScore," + "relationshipMatchModifierMentor,"
        + "relationshipMatchModifierMentorSeniorAuthor," + "relationshipMatchModifierManager," + "relationshipMatchModifierManagerSeniorAuthor" + "\n")
#capture misspelling key in the content
misspelling_list = []
count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            #the nested key structure might be different for every file, so we need to consider two conditions here
            if 'relationshipEvidenceTotalScore' not in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                print(personIdentifier, pmid)
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
