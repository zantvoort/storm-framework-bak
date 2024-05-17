/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package st.orm.template.impl;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import st.orm.Lazy;
import st.orm.template.SqlTemplateException;

import java.lang.reflect.RecordComponent;

/**
 * Bridge for creating lazy instances for records.
 */
public interface LazyFactory {

    Class<?> getPkType(@Nonnull RecordComponent component) throws SqlTemplateException;

    Lazy<?> create(@Nonnull RecordComponent component, @Nullable Object pk) throws SqlTemplateException;
}
