public class DownloadWpgFeedTasklet implements Tasklet {
  public static final Logger logger = LoggerFactory.getLogger(DownloadWpgFeedTasklet.class);

  @Value("${a2ca.wpg.batch.file-download-path}")
  public String fileDownloadPath;

  @Value("${a2ca.wpg.sftp.remote-dir}")
  public String sftpWpgRemoteDirectory;

  private final DefaultSftpSessionFactory defaultSftpSessionFactory;
  private final SftpOperationUtils sftpOperationUtils;
  private final JdbcTemplate jdbcTemplate;
  public static final String BRAND_BIN_RANGES_FILE_NAME = "brandBinRangesFileName";
  public static final String CO_BRAND_BIN_RANGES_FILE_NAME = "coBrandBinRangesFileName";

  public DownloadWpgFeedTasklet(
      @Qualifier("sftpSessionFactory") DefaultSftpSessionFactory defaultSftpSessionFactory,
      SftpOperationUtils sftpOperationUtils,
      JdbcTemplate jdbcTemplate) {
    this.defaultSftpSessionFactory = defaultSftpSessionFactory;
    this.sftpOperationUtils = sftpOperationUtils;
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  @Transactional
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws Exception {
    String brandBinRangesFileName =
        contribution.getStepExecution().getJobParameters().getString(BRAND_BIN_RANGES_FILE_NAME);
    String coBrandBinRangesFileName =
        contribution.getStepExecution().getJobParameters().getString(CO_BRAND_BIN_RANGES_FILE_NAME);
    int countOfProcessedBrandFileRecord = getCountOfProcessedFile(brandBinRangesFileName);
    int countOfProcessedCoBrandFileRecord = getCountOfProcessedFile(coBrandBinRangesFileName);

    if (!(countOfProcessedBrandFileRecord == 0 && countOfProcessedCoBrandFileRecord == 0)) {
      logger.error(
          "{}",
          logBuilder()
              .message(
                  "DownloadWPGFeed : Can not proceed because either brand, co-brand or both bin ranges file have already been processed"));
      throw new A2caWpgLoaderException(
          "Can not proceed because either brand, co-brand or both bin ranges file have already been processed",
          null);
    }

    SftpSession sftpSession = null;
    File brandBinRangesFile;
    File coBrandBinRangesFile;
    try {
      sftpSession = defaultSftpSessionFactory.getSession();
      brandBinRangesFile =
          sftpOperationUtils.downloadFile(
              sftpSession, sftpWpgRemoteDirectory, fileDownloadPath, brandBinRangesFileName);
      coBrandBinRangesFile =
          sftpOperationUtils.downloadFile(
              sftpSession, sftpWpgRemoteDirectory, fileDownloadPath, coBrandBinRangesFileName);
    } catch (Exception ex) {
      logger.error(
          "{}",
          logBuilder()
              .message(
                  "DownloadWPGFeed : Exception occurred while downloading either brand or co-brand bin ranges file"));
      throw new A2caWpgLoaderException(ex.getMessage(), ex);
    } finally {
      if (sftpSession != null) {
        sftpSession.close();
      }
    }

    ExecutionContext executionContext =
        contribution.getStepExecution().getJobExecution().getExecutionContext();
    executionContext.put("brandFilePath", brandBinRangesFile.getAbsolutePath());
    executionContext.put("brandFileName", brandBinRangesFile.getName());
    executionContext.put("coBrandFilePath", coBrandBinRangesFile.getAbsolutePath());
    executionContext.put("coBrandFileName", coBrandBinRangesFile.getName());
    insertUnProcessedFileInfo(brandBinRangesFileName);
    insertUnProcessedFileInfo(coBrandBinRangesFileName);
    logger.info(
        "{}",
        logBuilder()
            .message(
                String.format(
                    "DownloadWPGFeed : WPG brand bin ranges file %s and co-brand bin ranges file %s downloaded successfully.",
                    brandBinRangesFileName, coBrandBinRangesFileName)));
    return RepeatStatus.FINISHED;
  }
}
