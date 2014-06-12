/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

public class MultipartParser {

  private DiskFileItemFactory _uploadItemFactory;

  public MultipartParser(int spillToDiskSize) {
    _uploadItemFactory = new DiskFileItemFactory();
    _uploadItemFactory.setSizeThreshold(spillToDiskSize);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> parseMultipart(HttpServletRequest request)
      throws IOException, ServletException {
    ServletFileUpload upload = new ServletFileUpload(_uploadItemFactory);
    List<FileItem> items = null;
    try {
      items = upload.parseRequest(request);
    } catch (FileUploadException e) {
      throw new ServletException(e);
    }

    Map<String, Object> params = new HashMap<String, Object>();
    for (FileItem item : items) {
      if (item.isFormField())
        params.put(item.getFieldName(), item.getString());
      else
        params.put(item.getFieldName(), item);
    }
    return params;
  }

}
