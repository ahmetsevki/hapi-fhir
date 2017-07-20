package ca.uhn.fhir.jpa.search;

import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.google.common.annotations.VisibleForTesting;

import ca.uhn.fhir.jpa.dao.DaoConfig;
import ca.uhn.fhir.jpa.dao.data.*;
import ca.uhn.fhir.jpa.entity.Search;

/**
 * Deletes old searches
 */
public class StaleSearchDeletingSvcImpl implements IStaleSearchDeletingSvc {
	public static final long DEFAULT_CUTOFF_SLACK = 10 * DateUtils.MILLIS_PER_SECOND;
	private static final org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger(StaleSearchDeletingSvcImpl.class);

	/*
	 * We give a bit of extra leeway just to avoid race conditions where a query result
	 * is being reused (because a new client request came in with the same params) right before
	 * the result is to be deleted
	 */
	private long myCutoffSlack = DEFAULT_CUTOFF_SLACK;

	@Autowired
	private DaoConfig myDaoConfig;

	@Autowired
	private ISearchDao mySearchDao;

	@Autowired
	private ISearchIncludeDao mySearchIncludeDao;

	@Autowired
	private ISearchResultDao mySearchResultDao;

	@Autowired
	private PlatformTransactionManager myTransactionManager;

	private void deleteSearch(final Long theSearchPid) {
		Search searchToDelete = mySearchDao.findOne(theSearchPid);
		ourLog.info("Deleting search {}/{} - Created[{}] -- Last returned[{}]", searchToDelete.getId(), searchToDelete.getUuid(), searchToDelete.getCreated(), searchToDelete.getSearchLastReturned());
		mySearchIncludeDao.deleteForSearch(searchToDelete.getId());
		mySearchResultDao.deleteForSearch(searchToDelete.getId());
		mySearchDao.delete(searchToDelete);
	}

	@Override
	@Transactional(propagation = Propagation.NOT_SUPPORTED)
	public void pollForStaleSearchesAndDeleteThem() {

		long cutoffMillis = myDaoConfig.getExpireSearchResultsAfterMillis();
		if (myDaoConfig.getReuseCachedSearchResultsForMillis() != null) {
			cutoffMillis = Math.max(cutoffMillis, myDaoConfig.getReuseCachedSearchResultsForMillis());
		}
		final Date cutoff = new Date((System.currentTimeMillis() - cutoffMillis) - myCutoffSlack);

		ourLog.debug("Searching for searches which are before {}", cutoff);

		TransactionTemplate tt = new TransactionTemplate(myTransactionManager);
		int count = tt.execute(new TransactionCallback<Integer>() {
			@Override
			public Integer doInTransaction(TransactionStatus theStatus) {
				Slice<Long> toDelete = mySearchDao.findWhereLastReturnedBefore(cutoff, new PageRequest(0, 1000));
				for (final Long next : toDelete) {
					deleteSearch(next);
				}
				return toDelete.getContent().size();
			}
		});

		long total = tt.execute(new TransactionCallback<Long>() {
			@Override
			public Long doInTransaction(TransactionStatus theStatus) {
				return mySearchDao.count();
			}
		});

		ourLog.info("Deleted {} searches, {} remaining", count, total);

	}

	@Scheduled(fixedDelay = DEFAULT_CUTOFF_SLACK)
	@Transactional(propagation = Propagation.NOT_SUPPORTED)
	@Override
	public synchronized void schedulePollForStaleSearches() {
		if (!myDaoConfig.isSchedulingDisabled()) {
			if (myDaoConfig.isExpireSearchResults()) {
				pollForStaleSearchesAndDeleteThem();
			}
		}
	}

	@VisibleForTesting
	public void setCutoffSlackForUnitTest(long theCutoffSlack) {
		myCutoffSlack = theCutoffSlack;
	}

}
