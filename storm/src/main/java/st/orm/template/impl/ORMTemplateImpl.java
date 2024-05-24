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
import jakarta.persistence.PersistenceException;
import st.orm.BindVars;
import st.orm.FK;
import st.orm.Inline;
import st.orm.Lazy;
import st.orm.Name;
import st.orm.PK;
import st.orm.Persist;
import st.orm.Query;
import st.orm.Version;
import st.orm.repository.Column;
import st.orm.repository.Entity;
import st.orm.repository.EntityModel;
import st.orm.spi.EntityRepositoryProvider;
import st.orm.spi.ORMReflection;
import st.orm.spi.Providers;
import st.orm.template.ColumnNameResolver;
import st.orm.template.ForeignKeyResolver;
import st.orm.template.ORMTemplate;
import st.orm.template.QueryBuilder;
import st.orm.template.SqlTemplateException;
import st.orm.template.TableNameResolver;
import st.orm.template.TemplateFunction;
import st.orm.template.impl.SqlTemplateImpl.Eval;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.StringTemplate.RAW;
import static java.util.Objects.requireNonNull;
import static st.orm.Templates.unsafe;
import static st.orm.template.impl.SqlTemplateImpl.getColumnName;
import static st.orm.template.impl.SqlTemplateImpl.getForeignKey;
import static st.orm.template.impl.SqlTemplateImpl.getLazyRecordType;

class ORMTemplateImpl implements ORMTemplate {
    private static final ORMReflection REFLECTION = Providers.getORMReflection();

    private final QueryFactory factory;
    protected final TableNameResolver tableNameResolver;
    protected final ColumnNameResolver columnNameResolver;
    protected final ForeignKeyResolver foreignKeyResolver;
    protected final Predicate<? super EntityRepositoryProvider> providerFilter;

    public ORMTemplateImpl(@Nonnull QueryFactory factory,
                           @Nullable TableNameResolver tableNameResolver,
                           @Nullable ColumnNameResolver columnNameResolver,
                           @Nullable ForeignKeyResolver foreignKeyResolver,
                           @Nullable Predicate<? super EntityRepositoryProvider> providerFilter) {
        this.factory = requireNonNull(factory);
        this.tableNameResolver = tableNameResolver;
        this.columnNameResolver = columnNameResolver;
        this.foreignKeyResolver = foreignKeyResolver;
        this.providerFilter = providerFilter;
    }

    @Override
    public Query template(@Nonnull TemplateFunction function) {
        List<StringTemplate> templates = new ArrayList<>();
        String sql = function.interpolate(o -> {
            templates.add(RAW."\{new Eval(o)}");
            return "%s";
        });
        return process(StringTemplate.combine(RAW."\{unsafe(sql)}", StringTemplate.combine(templates)));
    }

    @Override
    public Query process(@Nonnull StringTemplate stringTemplate) throws PersistenceException {
        return factory.create(new LazyFactoryImpl(factory, tableNameResolver, columnNameResolver, foreignKeyResolver, providerFilter), stringTemplate);
    }

    @Override
    public BindVars createBindVars() {
        return factory.createBindVars();
    }

    @Override
    public <T extends Entity<ID>, ID> EntityModel<T, ID> model(@Nonnull Class<T> type) {
        return createEntityModel(type);
    }

    @Override
    public <T extends Record> QueryBuilder<T, T, Object> query(@Nonnull Class<T> recordType) {
        return new QueryBuilderImpl<>(this, recordType, recordType);
    }

    private <T extends Entity<ID>, ID> EntityModel<T, ID> createEntityModel(@Nonnull Class<T> type) throws PersistenceException {
        if (!type.isRecord()) {
            throw new PersistenceException(STR."Entity type must be a record: \{type.getSimpleName()}.");
        }
        List<Column> columns = Stream.of(type.getRecordComponents())
                .flatMap(c -> {
                    PK pk = REFLECTION.getAnnotation(c, PK.class);
                    return createColumns(c, pk != null, pk != null && pk.autoGenerated()).stream();
                })
                .toList();
        String tableName;
        Name name = REFLECTION.getAnnotation(type, Name.class);
        if (name != null) {
            tableName = name.value();
        } else if (tableNameResolver != null) {
            //noinspection unchecked
            tableName = tableNameResolver.resolveTableName((Class<? extends Record>) type);
        } else {
            tableName = type.getSimpleName();
        }
        //noinspection unchecked
        Class<ID> pkType =  (Class<ID>) REFLECTION.findPKType((Class<? extends Record>) type)
                .orElseThrow(() -> new PersistenceException(STR."No primary key found for entity type: \{type.getSimpleName()}."));
        return new EntityModel<>(tableName, type, pkType, columns);
    }

    private List<Column> createColumns(@Nonnull RecordComponent component, boolean primaryKey, boolean autoGenerated) {
        try {
            Class<?> componentType = component.getType();
            if (componentType.isRecord()) {
                if (REFLECTION.isAnnotationPresent(component, PK.class) || REFLECTION.isAnnotationPresent(component, Inline.class)) {
                    return Stream.of(componentType.getRecordComponents())
                            .flatMap(c -> createColumns(c, primaryKey, autoGenerated).stream())
                            .toList();
                }
            }
            boolean foreignKey = REFLECTION.isAnnotationPresent(component, FK.class);
            String columnName = foreignKey
                    ? getForeignKey(component, foreignKeyResolver)
                    : getColumnName(component, columnNameResolver);
            boolean nullable = !REFLECTION.isNonnull(component);
            Persist persist = REFLECTION.getAnnotation(component, Persist.class);
            boolean version = REFLECTION.isAnnotationPresent(component, Version.class);
            boolean insertable = persist == null || persist.insertable();
            boolean updatable = persist == null || persist.updatable();
            boolean lazy = Lazy.class.isAssignableFrom(componentType);
            if (lazy) {
                try {
                    componentType = getLazyRecordType(component);
                } catch (SqlTemplateException e) {
                    throw new PersistenceException(e);
                }
            }
            return List.of(new Column(columnName, componentType, primaryKey, autoGenerated, foreignKey, nullable, insertable, updatable, version, lazy));
        } catch (SqlTemplateException e) {
            throw new PersistenceException(e);
        }
    }
}
