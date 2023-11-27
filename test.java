 public SftpClient.DirEntry getLatestModifiedFile(
      SftpSession sftpSession, String sftpRemoteDirectory, String fileSearchString)
      throws IOException {
    List<SftpClient.DirEntry> dirEntries = sftpSession.doList(sftpRemoteDirectory).toList();
    List<SftpClient.DirEntry> filteredEntries =
        dirEntries.stream()
            .filter(dirEntry -> dirEntry.getFilename().startsWith(fileSearchString))
            .sorted(
                Comparator.comparing(
                    dirEntry -> dirEntry.getAttributes().getModifyTime(),
                    Comparator.reverseOrder()))
            .toList();
    SftpClient.DirEntry latestModifiedFile = null;
    if (!filteredEntries.isEmpty()) {
      latestModifiedFile = filteredEntries.get(0);
    }
    return latestModifiedFile;
  }
