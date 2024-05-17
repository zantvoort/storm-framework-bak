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
package st.orm.template;

import jakarta.annotation.Nonnull;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import st.orm.Templates;
import st.orm.template.impl.JpaTemplateImpl;

public interface JpaTemplate extends Templates, StringTemplate.Processor<Query, PersistenceException> {

    static JpaTemplate of(@Nonnull EntityManager entityManager) {
        return new JpaTemplateImpl(entityManager);
    }

    static ORMTemplate ORM(@Nonnull EntityManager entityManager) {
        return new JpaTemplateImpl(entityManager).toORM();
    }

    ORMTemplate toORM();
}
