#
# SQL Export
# Created by Querious (201054)
# Created: August 22, 2019 at 4:30:35 PM EDT
# Encoding: Unicode (UTF-8)
#

CREATE DATABASE IF NOT EXISTS `reciter` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci;
USE `reciter`;




SET @PREVIOUS_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS;
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS `person` (
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


CREATE TABLE IF NOT EXISTS `personArticle` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `totalArticleScoreStandardized` int(11) DEFAULT 0,
  `totalArticleScoreNonStandardized` float DEFAULT 0,
  `userAssertion` varchar(128) DEFAULT NULL,
  `publicationDateStandardized` varchar(128) DEFAULT NULL,
  `publicationTypeCanonical` varchar(128) DEFAULT NULL,
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
  `clusterIdentifier` int(11) DEFAULT 0,
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


CREATE TABLE IF NOT EXISTS `personArticleAuthor` (
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


CREATE TABLE IF NOT EXISTS `personArticleDepartment` (
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


CREATE TABLE IF NOT EXISTS `personArticleGrant` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `articleGrant` varchar(128) DEFAULT NULL,
  `grantMatchScore` float DEFAULT 0,
  `institutionGrant` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25902 DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `personArticleRelationship` (
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


CREATE TABLE IF NOT EXISTS `personArticleScopusNonTargetAuthorAffiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `nonTargetAuthorInstitutionLabel` varchar(128) DEFAULT 'NULL',
  `nonTargetAuthorInstitutionID` varchar(128) DEFAULT 'NULL',
  `nonTargetAuthorInstitutionCount` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=155674 DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `personArticleScopusTargetAuthorAffiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `targetAuthorInstitutionalAffiliationSource` varchar(128) DEFAULT 'NULL',
  `scopusTargetAuthorInstitutionalAffiliationIdentity` varchar(128) DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationArticleScopusLabel` varchar(2000) DEFAULT '''NULL''',
  `targetAuthorInstitutionalAffiliationArticleScopusAffiliationId` varchar(128) DEFAULT 'NULL',
  `targetAuthorInstitutionalAffiliationMatchType` varchar(128) DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationMatchTypeScore` float DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=292473 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `altmetric` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `doi` varchar(128) DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `altmetric_jid` varchar(128) DEFAULT NULL,
  `context-all-count` int(11) DEFAULT NULL,
  `context-all-mean` float(10,4) DEFAULT NULL,
  `context-all-rank` int(11) DEFAULT NULL,
  `context-all-pct` int(11) DEFAULT NULL,
  `context-all-higher_than` int(11) DEFAULT NULL,
  `context-similar_age_3m-count` int(11) DEFAULT 0,
  `context-similar_age_3m-mean` float(10,4) DEFAULT 0.0000,
  `context-similar_age_3m-rank` int(11) DEFAULT 0,
  `context-similar_age_3m-pct` int(11) DEFAULT 0,
  `context-similar_age_3m-higher_than` int(11) DEFAULT 0,
  `altmetric_id` int(11) DEFAULT NULL,
  `cited_by_msm_count` int(11) DEFAULT NULL,
  `is_oa` varchar(12) DEFAULT NULL,
  `cited_by_posts_count` int(11) DEFAULT NULL,
  `cited_by_tweeters_count` int(11) DEFAULT NULL,
  `cited_by_feeds_count` int(11) DEFAULT NULL,
  `cited_by_fbwalls_count` int(11) DEFAULT NULL,
  `cited_by_rh_count` int(11) DEFAULT NULL,
  `cited_by_accounts_count` int(11) DEFAULT NULL,
  `last_updated` int(11) DEFAULT NULL,
  `score` float(10,4) DEFAULT NULL,
  `history-1y` float(10,4) DEFAULT NULL,
  `history-6m` float(10,4) DEFAULT NULL,
  `history-3m` float(10,4) DEFAULT NULL,
  `history-1m` float(10,4) DEFAULT NULL,
  `history-1w` float(10,4) DEFAULT NULL,
  `history-at` float(10,4) DEFAULT NULL,
  `added_on` int(11) DEFAULT NULL,
  `published_on` int(11) DEFAULT NULL,
  `readers-mendeley` int(11) DEFAULT NULL,
  `createTimestamp` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `y` (`doi`) USING BTREE,
  KEY `x` (`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=10684 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `analysis_rcr` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT 0,
  `year` int(11) DEFAULT NULL,
  `is_research_article` varchar(11) DEFAULT NULL,
  `is_clinical` varchar(11) DEFAULT NULL,
  `relative_citation_ratio` float(6,2) DEFAULT NULL,
  `nih_percentile` float(5,2) DEFAULT NULL,
  `citation_count` int(11) DEFAULT NULL,
  `citations_per_year` float(7,3) DEFAULT NULL,
  `expected_citations_per_year` float(7,3) DEFAULT NULL,
  `field_citation_rate` float(7,3) DEFAULT NULL,
  `provisional` varchar(12) DEFAULT NULL,
  `doi` varchar(150) DEFAULT NULL,
  `human` float(4,2) DEFAULT NULL,
  `animal` float(4,2) DEFAULT NULL,
  `molecular_cellular` float(4,2) DEFAULT NULL,
  `apt` float(4,2) DEFAULT NULL,
  `x_coord` float(5,4) DEFAULT NULL,
  `y_coord` float(5,4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `sdfsdfsdf` (`pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=183597 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `analysis_rcr_cites` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `citing_pmid` int(11) DEFAULT 0,
  `cited_pmid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `y` (`cited_pmid`) USING BTREE,
  KEY `x` (`citing_pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=16235502 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `analysis_rcr_cites_clin` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `citing_pmid` int(11) DEFAULT 0,
  `cited_pmid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `y` (`cited_pmid`) USING BTREE,
  KEY `x` (`citing_pmid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=502068 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `analysis_rcr_summary` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(20) DEFAULT NULL,
  `nameFirst` varchar(128) DEFAULT NULL,
  `nameMiddle` varchar(128) DEFAULT NULL,
  `nameLast` varchar(128) DEFAULT NULL,
  `facultyRank` varchar(25) DEFAULT NULL,
  `department` varchar(128) DEFAULT NULL,
  `division` varchar(128) DEFAULT NULL,
  `countAll` int(11) DEFAULT 0,
  `countFirst` int(11) DEFAULT 0,
  `countSenior` int(11) DEFAULT 0,
  `top10PercentileAll` float(6,3) DEFAULT NULL,
  `top10RankAll` int(11) DEFAULT NULL,
  `top10DenominatorAll` int(11) DEFAULT NULL,
  `top5PercentileAll` float(7,3) DEFAULT NULL,
  `top5RankAll` int(11) DEFAULT NULL,
  `top5DenominatorAll` int(11) DEFAULT NULL,
  `top10PercentileFirst` float(6,3) DEFAULT NULL,
  `top10RankFirst` int(11) DEFAULT NULL,
  `top10DenominatorFirst` int(11) DEFAULT NULL,
  `top5PercentileFirst` float(7,3) DEFAULT NULL,
  `top5RankFirst` int(11) DEFAULT NULL,
  `top5DenominatorFirst` int(11) DEFAULT NULL,
  `top10PercentileSenior` float(6,3) DEFAULT NULL,
  `top10RankSenior` int(11) DEFAULT NULL,
  `top10DenominatorSenior` int(11) DEFAULT NULL,
  `top5PercentileSenior` float(7,3) DEFAULT NULL,
  `top5RankSenior` int(11) DEFAULT NULL,
  `top5DenominatorSenior` int(11) DEFAULT NULL,
  `top10PercentileFirstSenior` float(6,3) DEFAULT NULL,
  `top10RankFirstSenior` int(11) DEFAULT NULL,
  `top10DenominatorFirstSenior` int(11) DEFAULT NULL,
  `top5PercentileFirstSenior` float(7,3) DEFAULT NULL,
  `top5RankFirstSenior` int(11) DEFAULT NULL,
  `top5DenominatorFirstSenior` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4096 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `analysis_rcr_year` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(20) DEFAULT NULL,
  `year` int(11) NOT NULL DEFAULT 0,
  `aggregateRCR` float(5,2) DEFAULT 0.00,
  `personLabel` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE,
  KEY `idx_year` (`year`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12841 DEFAULT CHARSET=utf8mb4;



SET FOREIGN_KEY_CHECKS = @PREVIOUS_FOREIGN_KEY_CHECKS;


