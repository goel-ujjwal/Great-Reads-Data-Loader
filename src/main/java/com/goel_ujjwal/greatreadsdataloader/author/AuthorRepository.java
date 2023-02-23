package com.goel_ujjwal.greatreadsdataloader.author;

import org.springframework.data.cassandra.repository.CassandraRepository;

// Repository class to read/write "author_by_id" table

public interface AuthorRepository extends CassandraRepository<Author, String> {

}
