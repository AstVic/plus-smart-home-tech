package ru.yandex.practicum.collector.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GeneralAvroSerializer implements Serializer<SpecificRecordBase> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public byte[] serialize(String topic, SpecificRecordBase specificRecordBase) {
        if (specificRecordBase == null) {
            return null;
        }

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<SpecificRecordBase> datumWriter = new SpecificDatumWriter<>(specificRecordBase.getSchema());
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);
            datumWriter.write(specificRecordBase, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Ошибка сериализации Avro в топике " + topic, e);
        }
    }
}
