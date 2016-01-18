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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.server.session.Session;

public class FileEditorServlet extends LoginAbstractAzkabanServlet {

	private static final long serialVersionUID = 1L;

	@Override
	protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
		Page page = newPage(req, resp, session, "azkaban/webapp/servlet/velocity/fileeditor.vm");
		page.render();
	}

	@Override
	protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
			throws ServletException, IOException {
	}

}
