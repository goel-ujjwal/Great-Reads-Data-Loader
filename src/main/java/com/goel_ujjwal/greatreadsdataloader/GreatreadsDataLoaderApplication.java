package com.goel_ujjwal.greatreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.goel_ujjwal.greatreadsdataloader.author.Author;
import com.goel_ujjwal.greatreadsdataloader.author.AuthorRepository;
import com.goel_ujjwal.greatreadsdataloader.book.Book;
import com.goel_ujjwal.greatreadsdataloader.book.BookRepository;
import com.goel_ujjwal.greatreadsdataloader.connection.DataStaxAstraProperties;

// Class with main() for running the Spring Boot application

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class GreatreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.authors}")
	private String authorsDumpLocation;
	
	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(GreatreadsDataLoaderApplication.class, args);
	}

	// method to load author data into "author_by_id" table
	private void initAuthors() {
		Path path = Paths.get(authorsDumpLocation);

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct author object
					Author author = new Author();
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));

					// Persist using Repository
					authorRepository.save(author);

				} catch (JSONException e) {
					e.printStackTrace();
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// method to load book data into "book_by_id" table
	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);

		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					// Construct work object
					Book book = new Book();

					book.setId(jsonObject.optString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));

					JSONObject descriptionObject = jsonObject.optJSONObject("description");
					if(descriptionObject != null) {
						book.setDescription(descriptionObject.optString("value"));
					}

					JSONObject publishedObject = jsonObject.optJSONObject("created");
					if(publishedObject != null) {
						String dateStr = publishedObject.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					JSONArray coversJSONArray = jsonObject.optJSONArray("covers");
					if(coversJSONArray != null) {
						List<String> coverIds = new ArrayList<>();
						for(int i = 0; i < coversJSONArray.length(); i++) {
							coverIds.add(coversJSONArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsJSONArray = jsonObject.optJSONArray("authors");
					if(authorsJSONArray != null) {
						List<String> authorIds = new ArrayList<>();
						for(int i = 0; i < authorsJSONArray.length(); i++) {
							String authorId = authorsJSONArray.getJSONObject(i).getJSONObject("author").getString("key")
															  .replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);

						// fetch author_name using author_id in author_by_id cassandra table
						List<String> auhtorNames = authorIds.stream().map(id -> authorRepository.findById(id))
															.map(optionalAuthor -> {
																if(!optionalAuthor.isPresent()) return "Unknown Author";
																return optionalAuthor.get().getName();
															}).collect(Collectors.toList());
						book.setAuthorNames(auhtorNames);											
					}

					// Persist using Repository
					bookRepository.save(book);

				} catch (Exception e) {
					e.printStackTrace();
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {

		initAuthors();
		initWorks(); 
	}	

	// Using secure-bundle to connect to our DataStax Astra Cassandra database
	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
