/*
 * Copyright (C) 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package azkaban.crypto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import com.google.common.base.Preconditions;

public class Decryptions {
  private static final FsPermission USER_READ_PERMISSION_ONLY = new FsPermission(FsAction.READ,
                                                                                 FsAction.NONE,
                                                                                 FsAction.NONE);

  public String decrypt(String cipheredText, String passphrasePath, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(cipheredText);
    Preconditions.checkNotNull(passphrasePath);

    Path path = new Path(passphrasePath);
    Preconditions.checkArgument(fs.exists(path), "File does not exist at " + passphrasePath);
    Preconditions.checkArgument(fs.isFile(path), "Passphrase path is not a file. " + passphrasePath);

    FileStatus fileStatus = fs.getFileStatus(path);
    Preconditions.checkArgument(USER_READ_PERMISSION_ONLY.equals(fileStatus.getPermission()),
                                "Passphrase file should only have read only permission on only user. " + passphrasePath);

    Crypto crypto = new Crypto();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
      String passphrase = br.readLine();
      String decrypted = crypto.decrypt(cipheredText, passphrase);
      Preconditions.checkNotNull(decrypted, "Was not able to decrypt");
      return decrypted;
    }
  }
}
