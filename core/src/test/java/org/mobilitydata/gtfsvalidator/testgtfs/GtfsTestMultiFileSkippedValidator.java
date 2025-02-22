/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mobilitydata.gtfsvalidator.testgtfs;

import javax.inject.Inject;
import org.mobilitydata.gtfsvalidator.notice.NoticeContainer;
import org.mobilitydata.gtfsvalidator.validator.FileValidator;

public class GtfsTestMultiFileSkippedValidator extends FileValidator {

  private final GtfsTestTableContainer table;
  private final GtfsTestTableContainer2 unparsableTable;

  @Inject
  public GtfsTestMultiFileSkippedValidator(
      GtfsTestTableContainer table, GtfsTestTableContainer2 table2) {
    this.table = table;
    this.unparsableTable = table2;
  }

  @Override
  public void validate(NoticeContainer noticeContainer) {}

  @Override
  public boolean shouldCallValidate() {
    return false;
  }
}
