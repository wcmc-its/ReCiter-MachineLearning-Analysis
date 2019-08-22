#
# SQL Export
# Created by Querious (201054)
# Created: August 22, 2019 at 4:30:35 PM EDT
# Encoding: Unicode (UTF-8)
#


DROP DATABASE IF EXISTS `reciter`;
CREATE DATABASE `reciter` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;
USE `reciter`;




SET @PREVIOUS_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS;
SET FOREIGN_KEY_CHECKS = 0;


DROP TABLE IF EXISTS `personArticleScopusTargetAuthorAffiliation`;
DROP TABLE IF EXISTS `personArticleScopusNonTargetAuthorAffiliation`;
DROP TABLE IF EXISTS `personArticleRelationship`;
DROP TABLE IF EXISTS `personArticleGrant`;
DROP TABLE IF EXISTS `personArticleDepartment`;
DROP TABLE IF EXISTS `personArticleAuthor`;
DROP TABLE IF EXISTS `personArticle`;
DROP TABLE IF EXISTS `person`;


CREATE TABLE `person` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `dateAdded` varchar(128) DEFAULT NULL,
  `dateUpdated` varchar(128) DEFAULT NULL,
  `precision` float DEFAULT 0,
  `recall` float DEFAULT 0,
  `countSuggestedArticles` int(11) DEFAULT 0,
  `overallAccuracy` float DEFAULT 0,
  `mode` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11633 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticle` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `totalArticleScoreStandardized` int(11) DEFAULT 0,
  `totalArticleScoreNonStandardized` float DEFAULT 0,
  `userAssertion` varchar(128) DEFAULT NULL,
  `publicationDateStandardized` varchar(128) DEFAULT NULL,
  `publicationTypeCanonical` varchar(128) DEFAULT NULL,
  `publicationAbstract` text DEFAULT NULL,
  `scopusDocID` varchar(128) DEFAULT NULL,
  `journalTitleVerbose` varchar(500) DEFAULT 'NULL',
  `articleTitle` varchar(1000) DEFAULT '''NULL''',
  `feedbackScoreAccepted` float DEFAULT NULL,
  `feedbackScoreRejected` float DEFAULT NULL,
  `feedbackScoreNull` float DEFAULT NULL,
  `articleAuthorNameFirstName` varchar(128) DEFAULT '''NULL''',
  `articleAuthorNameLastName` varchar(128) DEFAULT '''NULL''',
  `institutionalAuthorNameFirstName` varchar(128) DEFAULT '''NULL''',
  `institutionalAuthorNameMiddleName` varchar(128) DEFAULT '''NULL''',
  `institutionalAuthorNameLastName` varchar(128) DEFAULT '''NULL''',
  `nameMatchFirstScore` float DEFAULT 0,
  `nameMatchFirstType` varchar(128) DEFAULT 'NULL',
  `nameMatchMiddleScore` float DEFAULT 0,
  `nameMatchMiddleType` varchar(128) DEFAULT 'NULL',
  `nameMatchLastScore` float DEFAULT 0,
  `nameMatchLastType` varchar(128) DEFAULT 'NULL',
  `nameMatchModifierScore` float DEFAULT 0,
  `nameScoreTotal` float DEFAULT 0,
  `emailMatch` varchar(128) DEFAULT NULL,
  `emailMatchScore` float DEFAULT NULL,
  `journalSubfieldScienceMetrixLabel` varchar(128) DEFAULT 'NULL',
  `journalSubfieldScienceMetrixID` varchar(128) DEFAULT NULL,
  `journalSubfieldDepartment` varchar(128) DEFAULT NULL,
  `journalSubfieldScore` float DEFAULT 0,
  `relationshipEvidenceTotalScore` float DEFAULT 0,
  `relationshipMinimumTotalScore` float DEFAULT 0,
  `relationshipNonMatchCount` int(11) DEFAULT 0,
  `relationshipNonMatchScore` float DEFAULT 0,
  `articleYear` int(11) DEFAULT 0,
  `identityBachelorYear` varchar(128) DEFAULT 'NULL',
  `discrepancyDegreeYearBachelor` int(11) DEFAULT 0,
  `discrepancyDegreeYearBachelorScore` float DEFAULT 0,
  `identityDoctoralYear` varchar(128) DEFAULT NULL,
  `discrepancyDegreeYearDoctoral` int(11) DEFAULT 0,
  `discrepancyDegreeYearDoctoralScore` float DEFAULT 0,
  `genderScoreArticle` float DEFAULT 0,
  `genderScoreIdentity` float DEFAULT 0,
  `genderScoreIdentityArticleDiscrepancy` float DEFAULT 0,
  `personType` varchar(128) DEFAULT NULL,
  `personTypeScore` float DEFAULT NULL,
  `countArticlesRetrieved` int(11) DEFAULT 0,
  `articleCountScore` float DEFAULT 0,
  `targetAuthorInstitutionalAffiliationArticlePubmedLabel` text DEFAULT NULL,
  `pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore` float DEFAULT NULL,
  `scopusNonTargetAuthorInstitutionalAffiliationSource` varchar(128) DEFAULT '''''''NULL''''''',
  `scopusNonTargetAuthorInstitutionalAffiliationScore` float DEFAULT 0,
  `totalArticleScoreWithoutClustering` float DEFAULT 0,
  `clusterScoreAverage` float DEFAULT 0,
  `clusterReliabilityScore` float DEFAULT 0,
  `clusterScoreModificationOfTotalScore` float DEFAULT 0,
  `datePublicationAddedToEntrez` varchar(128) DEFAULT NULL,
  `doi` varchar(128) DEFAULT NULL,
  `issn` varchar(128) DEFAULT NULL,
  `issue` varchar(500) DEFAULT 'NULL',
  `journalTitleISOabbreviation` varchar(128) DEFAULT NULL,
  `pages` varchar(500) DEFAULT 'NULL',
  `timesCited` int(11) DEFAULT NULL,
  `volume` varchar(500) DEFAULT 'NULL',
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=303693 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleAuthor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `authorFirstName` varchar(128) DEFAULT '''NULL''',
  `authorLastName` varchar(128) DEFAULT '''NULL''',
  `targetAuthor` varchar(128) DEFAULT '''NULL''',
  `rank` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3031327 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleDepartment` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `identityOrganizationalUnit` varchar(128) DEFAULT NULL,
  `articleAffiliation` varchar(10000) DEFAULT 'NULL',
  `organizationalUnitType` varchar(128) DEFAULT NULL,
  `organizationalUnitMatchingScore` float DEFAULT 0,
  `organizationalUnitModifier` varchar(128) DEFAULT NULL,
  `organizationalUnitModifierScore` float DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=60913 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleGrant` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `articleGrant` varchar(128) DEFAULT NULL,
  `grantMatchScore` float DEFAULT 0,
  `institutionGrant` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25902 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleRelationship` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `relationshipNameArticleFirstName` varchar(128) DEFAULT '''NULL''',
  `relationshipNameArticleLastName` varchar(128) DEFAULT '''NULL''',
  `relationshipNameIdentityFirstName` varchar(128) DEFAULT '''NULL''',
  `relationshipNameIdentityLastName` varchar(128) DEFAULT '''NULL''',
  `relationshipType` varchar(128) DEFAULT NULL,
  `relationshipMatchType` varchar(128) DEFAULT NULL,
  `relationshipMatchingScore` float DEFAULT 0,
  `relationshipVerboseMatchModifierScore` float DEFAULT 0,
  `relationshipMatchModifierMentor` float DEFAULT NULL,
  `relationshipMatchModifierMentorSeniorAuthor` float DEFAULT NULL,
  `relationshipMatchModifierManager` float DEFAULT NULL,
  `relationshipMatchModifierManagerSeniorAuthor` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=78525 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleScopusNonTargetAuthorAffiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `nonTargetAuthorInstitutionLabel` varchar(128) DEFAULT 'NULL',
  `nonTargetAuthorInstitutionID` varchar(128) DEFAULT 'NULL',
  `nonTargetAuthorInstitutionCount` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=155674 DEFAULT CHARSET=utf8mb4;


CREATE TABLE `personArticleScopusTargetAuthorAffiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` varchar(128) DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationSource` varchar(128) DEFAULT 'NULL',
  `scopusTargetAuthorInstitutionalAffiliationIdentity` varchar(128) DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationArticleScopusLabel` varchar(2000) DEFAULT '''NULL''',
  `targetAuthorInstitutionalAffiliationArticleScopusAffiliationId` varchar(128) DEFAULT 'NULL',
  `targetAuthorInstitutionalAffiliationMatchType` varchar(128) DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationMatchTypeScore` float DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=292473 DEFAULT CHARSET=utf8mb4;




SET FOREIGN_KEY_CHECKS = @PREVIOUS_FOREIGN_KEY_CHECKS;


