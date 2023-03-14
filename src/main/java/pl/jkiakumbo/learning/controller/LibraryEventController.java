package pl.jkiakumbo.learning.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pl.jkiakumbo.learning.DataBase;
import pl.jkiakumbo.learning.domain.LibraryEvent;
import pl.jkiakumbo.learning.domain.LibraryEventType;
import pl.jkiakumbo.learning.producers.LibraryEventProducer;

import java.util.List;

@RestController
@RequestMapping("/events")
@Slf4j
public class LibraryEventController {

    private LibraryEventProducer libraryEventProducer;
    private DataBase dataBase;

    @Autowired
    public LibraryEventController(LibraryEventProducer libraryEventProducer, DataBase dataBase) {
        this.libraryEventProducer = libraryEventProducer;
        this.dataBase = dataBase;
    }

    @GetMapping("/{id}")
    public ResponseEntity<LibraryEvent> getLibraryEvents(@PathVariable Integer id){
        LibraryEvent libraryEvent = dataBase.find(id);

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    @GetMapping
    public ResponseEntity<List<LibraryEvent>> getLibraryEvents(){
        List<LibraryEvent> libraryEvents = dataBase.findAll();

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvents);
    }

    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent){

        libraryEvent = libraryEvent.setType(LibraryEventType.NEW);
        return getLibraryEventResponseEntity(libraryEvent);
    }

    @PutMapping
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent){

        libraryEvent = libraryEvent.setType(LibraryEventType.UPDATE);
        return getLibraryEventResponseEntity(libraryEvent);
    }

    private ResponseEntity<LibraryEvent> getLibraryEventResponseEntity(LibraryEvent libraryEvent) {
        try {
            libraryEventProducer.sendLibraryEvent(libraryEvent);
        } catch (JsonProcessingException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        dataBase.save(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


}
