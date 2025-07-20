/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.robux.storage;

import org.apache.robux.error.RobuxException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class NilStorageConnector implements StorageConnector
{
  private static final NilStorageConnector NIL_STORAGE_CONNECTOR = new NilStorageConnector();

  private NilStorageConnector()
  {

  }

  public static NilStorageConnector getInstance()
  {
    return NIL_STORAGE_CONNECTOR;
  }

  @Override
  public boolean pathExists(String path)
  {
    throw notConfiguredException();
  }

  @Override
  public InputStream read(String path)
  {
    throw notConfiguredException();

  }

  @Override
  public InputStream readRange(String path, long from, long size)
  {
    throw notConfiguredException();

  }

  @Override
  public OutputStream write(String path)
  {
    throw notConfiguredException();
  }

  @Override
  public void deleteFile(String path)
  {
    throw notConfiguredException();
  }

  @Override
  public void deleteFiles(Iterable<String> paths)
  {
    throw notConfiguredException();
  }

  @Override
  public void deleteRecursively(String path)
  {
    throw notConfiguredException();
  }

  @Override
  public Iterator<String> listDir(String dirName)
  {
    throw notConfiguredException();
  }

  private RobuxException notConfiguredException()
  {
    return RobuxException.forPersona(RobuxException.Persona.DEVELOPER)
                         .ofCategory(RobuxException.Category.DEFENSIVE)
                         .build("Please configure durable storage.");
  }
}
