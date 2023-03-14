package pl.jkiakumbo.learning;

import org.springframework.stereotype.Service;
import pl.jkiakumbo.learning.domain.LibraryEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DataBase {

    private final Map<Integer, LibraryEvent> events;

    public DataBase() {
        this.events = new HashMap<>();
    }

    public void save(LibraryEvent libraryEvent){
        events.put(libraryEvent.libraryEventId(), libraryEvent);
    }

    public LibraryEvent find(Integer libraryEventId){
        return events.get(libraryEventId);
    }

    public LibraryEvent delete(Integer libraryEventId){
        return events.remove(libraryEventId);
    }


    public List<LibraryEvent> findAll() {
        return events.entrySet().stream()
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
    }
}
