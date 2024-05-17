package st.orm.template;

import jakarta.annotation.Nonnull;
import jakarta.persistence.NoResultException;
import jakarta.persistence.NonUniqueResultException;
import jakarta.persistence.PersistenceException;
import st.orm.PreparedQuery;
import st.orm.Query;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public interface QueryBuilder<T, R, ID> extends StringTemplate.Processor<Stream<R>, PersistenceException> {

    // Don't let Builder extend Iterable<R>, because that would disallow us from closing the underlying stream.

    QueryBuilder<T, R, ID> distinct();

    interface TypedJoinBuilder<T, R, ID> extends JoinBuilder<T, R, ID> {

        QueryBuilder<T, R, ID> on(@Nonnull Class<? extends Record> relation);
    }

    interface JoinBuilder<T, R, ID> {

        OnBuilder<T, R, ID> on();
    }

    interface OnBuilder<T, R, ID> extends StringTemplate.Processor<QueryBuilder<T, R, ID>, PersistenceException> {

        QueryBuilder<T, R, ID> template(@Nonnull TemplateFunction function);
    }

    interface PredicateBuilder<T, R, ID> extends StringTemplate.Processor<WhereBuilder<T, R, ID>, PersistenceException> {

        WhereBuilder<T, R, ID> template(@Nonnull TemplateFunction function);

        WhereBuilder<T, R, ID> where(@Nonnull Object o);
    }

    interface WhereBuilder<T, R, ID> {

        WhereBuilder<T, R, ID> and(@Nonnull Function<PredicateBuilder<T, R, ID>, WhereBuilder<T, R, ID>> predicate);

        WhereBuilder<T, R, ID> or(@Nonnull Function<PredicateBuilder<T, R, ID>, WhereBuilder<T, R, ID>> predicate);

        WhereBuilder<T, R, ID> and(@Nonnull Object o);

        WhereBuilder<T, R, ID> or(@Nonnull Object o);
    }

    <X extends Record> QueryBuilder<T, X, ID> select(@Nonnull Class<X> resultType);

    <X> QueryBuilder<T, X, ID> selectTemplate(@Nonnull Class<X> resultType, @Nonnull TemplateFunction function);

    <X> StringTemplate.Processor<QueryBuilder<T, X, ID>, PersistenceException> selectTemplate(@Nonnull Class<X> resultType);

    QueryBuilder<T, R, ID> crossJoin(@Nonnull Class<? extends Record> relation);

    TypedJoinBuilder<T, R, ID> innerJoin(@Nonnull Class<? extends Record> relation);

    TypedJoinBuilder<T, R, ID> leftJoin(@Nonnull Class<? extends Record> relation);

    TypedJoinBuilder<T, R, ID> rightJoin(@Nonnull Class<? extends Record> relation);

    TypedJoinBuilder<T, R, ID> join(@Nonnull JoinType type, @Nonnull Class<? extends Record> relation, @Nonnull String alias);

    StringTemplate.Processor<QueryBuilder<T, R, ID>, PersistenceException> crossJoin();

    StringTemplate.Processor<JoinBuilder<T, R, ID>, PersistenceException> innerJoin(@Nonnull String alias);

    StringTemplate.Processor<JoinBuilder<T, R, ID>, PersistenceException> leftJoin(@Nonnull String alias);

    StringTemplate.Processor<JoinBuilder<T, R, ID>, PersistenceException> rightJoin(@Nonnull String alias);

    StringTemplate.Processor<JoinBuilder<T, R, ID>, PersistenceException> join(@Nonnull JoinType type, @Nonnull String alias);

    JoinBuilder<T, R, ID> join(@Nonnull JoinType type, @Nonnull String alias, @Nonnull TemplateFunction function);

    default QueryBuilder<T, R, ID> where(@Nonnull Object o) {
        return where(predicate -> predicate.where(o));
    }

    QueryBuilder<T, R, ID> where(@Nonnull Function<PredicateBuilder<T, R, ID>, WhereBuilder<T, R, ID>> expression);

    QueryBuilder<T, R, ID> withTemplate(@Nonnull TemplateFunction function);

    StringTemplate.Processor<QueryBuilder<T, R, ID>, PersistenceException> withTemplate();

    Query build();

    default PreparedQuery prepare() {
        return build().prepare();
    }

    Stream<R> stream(@Nonnull TemplateFunction function);

    Stream<R> stream();

    default List<R> toList() {
        return stream().toList();
    }

    default R singleResult() {
        return stream()
                .reduce((_, _) -> {
                    throw new NonUniqueResultException("Expected single result, but found more than one.");
                }).orElseThrow(() -> new NoResultException("Expected single result, but found none."));
    }

    default Optional<R> optionalResult() {
        return stream()
                .reduce((_, _) -> {
                    throw new NonUniqueResultException("Expected single result, but found more than one.");
                });
    }

    /**
     * Performs the function in multiple batches.
     *
     * @param stream the stream to batch.
     * @param function the function to apply to each batch.
     * @return a stream of results from each batch.
     * @param <X> the type of elements in the stream.
     * @param <Y> the type of elements in the result stream.
     */
    default <X, Y> Stream<Y> batch(@Nonnull Stream<X> stream, @Nonnull Function<Stream<X>, Stream<Y>> function) {
        return autoClose(slice(stream).flatMap(function)).onClose(stream::close);
    }

    /**
     * Performs the function in multiple batches, each containing up to {@code batchSize} elements from the stream.
     *
     * @param stream the stream to batch.
     * @param batchSize the maximum number of elements to include in each batch.
     * @param function the function to apply to each batch.
     * @return a stream of results from each batch.
     * @param <X> the type of elements in the stream.
     * @param <Y> the type of elements in the result stream.
     */
    default <X, Y> Stream<Y> batch(@Nonnull Stream<X> stream, int batchSize, @Nonnull Function<Stream<X>, Stream<Y>> function) {
        return autoClose(slice(stream, batchSize).flatMap(function)).onClose(stream::close);
    }

    /**
     * Wraps the stream in a stream that is automatically closed after a terminal operation.
     *
     * @param stream the stream to wrap.
     * @return a stream that is automatically closed after a terminal operation.
     * @param <X> the type of the stream.
     */
    <X> Stream<X> autoClose(@Nonnull Stream<X> stream);

    /**
     * Generates a stream of slices. This method is designed to facilitate batch processing of large streams by
     * dividing the stream into smaller manageable slices, which can be processed independently.
     *
     * The method utilizes a "tripwire" mechanism to ensure that the original stream is properly managed and closed upon
     * completion of processing, preventing resource leaks.
     *
     * @param <X> the type of elements in the stream.
     * @param stream the original stream of elements to be sliced.
     * {@code Integer.MAX_VALUE}, only one slice will be returned.
     * @return a stream of slices, where each slice contains up to {@code batchSize} elements from the original stream.
     */
    <X> Stream<Stream<X>> slice(@Nonnull Stream<X> stream);

    /**
     * Generates a stream of slices, each containing a subset of elements from the original stream up to a specified
     * size. This method is designed to facilitate batch processing of large streams by dividing the stream into
     * smaller manageable slices, which can be processed independently.
     *
     * If the specified size is equal to {@code Integer.MAX_VALUE}, this method will return a single slice containing
     * the original stream, effectively bypassing the slicing mechanism. This is useful for operations that can handle
     * all elements at once without the need for batching.
     *
     * The method utilizes a "tripwire" mechanism to ensure that the original stream is properly managed and closed upon
     * completion of processing, preventing resource leaks.
     *
     * @param <X> the type of elements in the stream.
     * @param stream the original stream of elements to be sliced.
     * @param size the maximum number of elements to include in each slice. If {@code size} is
     * {@code Integer.MAX_VALUE}, only one slice will be returned.
     * @return a stream of slices, where each slice contains up to {@code batchSize} elements from the original stream.
     */
    <X> Stream<Stream<X>> slice(@Nonnull Stream<X> stream, int size);
}
