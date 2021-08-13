package com.weserve.appointment.service.controller;

import com.weserve.appointment.service.exception.BusinessValidationException;
import com.weserve.appointment.service.model.dto.*;
import com.weserve.appointment.service.model.entity.AppointmentEntity;
import com.weserve.appointment.service.model.entity.AppointmentEntity_;
import com.weserve.appointment.service.model.entity.AppointmentTimeSlotEntity;
import com.weserve.appointment.service.model.entity.TruckVisitAppointment;
import com.weserve.appointment.service.model.entity.UnitEntity;
import com.weserve.appointment.service.services.AppointmentService;
import com.weserve.appointment.service.services.ApptStatusUpdateService;
import com.weserve.framework.base.model.entity.UserEntity;
import com.weserve.framework.hibernate.common.PersistenceInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


@RestController
@RequestMapping("/api")
public class AppointmentController implements ApplicationContextAware {

    @Autowired
    AppointmentService appointmentService;

    @Autowired
    private ApptStatusUpdateService apptStatusUpdateService;

    protected ApplicationContext applicationContext;

    //For Hone Integration
    /*@GetMapping(path = "getHoneApptTimeSlot")
    public List<AppointmentTimeSlotEntity> fetchHoneApptTimeSlot(@RequestBody List<AppointmentTimeSlotEntity> apptslotlists) throws ParseException {
        return appointmentService.findOrEnquireHoneAppointmentTimeSlot(apptslotlists);
    }*/

    @GetMapping(path = "getDeletedAppt")
    public List<AppointmentEntity> fetchAllDeletedTranAppointment() {
        return appointmentService.findAllDeletedAppointments();
    }

    @GetMapping(path = "getApptTimeSlot")
    public List<AppointmentTimeSlotEntity> fetchApptTimeSlot(@RequestParam String date, String trantype) throws ParseException {
        logger.debug("Current date to be processed recieved :: " + date + " :: Along with the tran type  :: " + trantype);
       /* List<AppointmentTimeSlotEntity> appointmentTimeSlotEntityList = new ArrayList<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy-MMM-dd");
        date = "20" + date;*/
        logger.debug("Current date to be processed recieved :: modified :: " + date);
        return appointmentService.findOrEnquireAppointmentTimeSlot(date, trantype);
    }

    @PostMapping(path = "updateAppointmentStatus")
    public void updateAppointmentStatus(@RequestBody AppointmentStatusDTO appointmentStatusDTO) {
        apptStatusUpdateService.updateAppointmentStatus(appointmentStatusDTO);
    }

    @ResponseBody
    @RequestMapping(value = "getHoneApptTimeSlots", method = RequestMethod.POST)
    public List<AppointmentTimeSlotEntity> getHoneApptTimeSlot(@RequestBody List<AppointmentTimeSlotEntity> apptslotlist) {
        return appointmentService.getHoneApptTimeSlots(apptslotlist);
    }

    @GetMapping(path = "getTranAppt")
    public List<AppointmentEntity> fetchAppointmentForSpecificSlot(@RequestParam Long slotId) {
        return appointmentService.findAppointmentForSpecificSlot(slotId);
    }

    @PostMapping(path = "getTransactionAppt")
    public AppointmentListDto FetchAllTransactionAppointments(@RequestBody FetchRequestDto fetchRequestDto) {
        Pageable pageable = PageRequest.of(fetchRequestDto.getPage(), fetchRequestDto.getSize(), Sort.by(AppointmentEntity_.APPT_NBR).descending());
        return appointmentService.findAllActiveAppointments(pageable, fetchRequestDto.getSortOrder(),
                fetchRequestDto.getSortColumn(), fetchRequestDto.getFilterDtoList());
    }

    @GetMapping(path = "fetchApptByTrkVisit")
    public List<AppointmentEntity> fetchApptByTrkVisit(@RequestParam String truckVisitApptKey) {
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        TruckVisitAppointment truckVisitAppointment = (TruckVisitAppointment) PersistenceInterface.getInstance().
                findById(TruckVisitAppointment.class, Long.valueOf(truckVisitApptKey));
        return appointmentService.findApptByTruckVisitAppt(truckVisitAppointment, userEntity);
    }

    @GetMapping(path = "fetchUnit")
    public UnitDetailsDTO fetchUnit(@RequestParam String unitId, @RequestParam String transactionType, @RequestParam String date) throws ExecutionException, InterruptedException {
        CompletableFuture<UnitEntity> responseMsgComplete;
        CompletableFuture<List<AppointmentTimeSlotEntity>> responseMsgComplete1;
        UnitDetailsDTO unitDetailsDTO = new UnitDetailsDTO();
        try {
            responseMsgComplete = appointmentService.findOrEnquireUnitDetailsAsync(unitId, transactionType, true);
            Date tempDate = new Date();
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MMM-dd HHmm");
            LocalDateTime now = LocalDateTime.now();
            String formattedDate = dtf.format(now);
            if (transactionType.equalsIgnoreCase("DI")) {
                    responseMsgComplete1 = appointmentService.findOrEnquireHoneAppointmentTimeSlotAsync(unitId, transactionType);

            /*if (responseMsgComplete1.get() == null || responseMsgComplete1.get().size() == 0) {
                responseMsgComplete1 = appointmentService.findOrEnquireTOSAppointmentTimeSlotAsync(formattedDate, transactionType);
            }*/
            } else if (transactionType.equalsIgnoreCase("RE") || transactionType.equalsIgnoreCase("RM")) {
                responseMsgComplete1 = appointmentService.findOrEnquireLocalAppointmentTimeSlotAsync(unitId, transactionType);
            } else {
                responseMsgComplete1 = appointmentService.findOrEnquireTOSAppointmentTimeSlotAsync(formattedDate, transactionType);
            }

            CompletableFuture.allOf(responseMsgComplete, responseMsgComplete1).join();
            try {
                UnitEntity unitEntity = Objects.requireNonNull(responseMsgComplete).get();
                List<AppointmentTimeSlotEntity> appointmentTimeSlotEntityList = responseMsgComplete1.get();
                if (unitEntity != null) {
                    unitDetailsDTO.setResponseMessage("Success");
                    unitDetailsDTO.setUnitEntity(unitEntity);
                    unitDetailsDTO.setAppointmentTimeSlotEntities(appointmentTimeSlotEntityList);
                    UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
                    unitDetailsDTO.setUserEntity(userEntity);
                } else {
                    unitDetailsDTO.setResponseMessage("Unable to find Container Details in TOS");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } catch (BusinessValidationException bve) {
            unitDetailsDTO.setResponseMessage(bve.getErrorMessage());
        }

        return unitDetailsDTO;
    }

    @PostMapping(value = "createAppointment")
    public ResponseEntity<String> createTranAppt(@RequestBody List<AppointmentEntity> appointmentList) throws ParseException, DatatypeConfigurationException, JAXBException {
        String responseMsg = null;
        List<AppointmentEntity> appointmentEntityList = new ArrayList<>();
        CompletableFuture<String> responseMsgComplete;
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        for (AppointmentEntity appointmentEntity : appointmentList) {
            responseMsgComplete = appointmentService.findOrCreateAppointment(appointmentEntity, userEntity);
            CompletableFuture.allOf(responseMsgComplete).join();
            try {
                responseMsg = responseMsgComplete.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return new ResponseEntity<String>(responseMsg, HttpStatus.OK);
    }

    @PostMapping(value = "createExportEmptyAppointment")
    public ResponseEntity<String> createExportEmptyAppointment(@RequestBody List<DualAppointmentDTO> appointmentDTOList) {
        String responseMsg = null;
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        CompletableFuture<String> responseMsgComplete;
        for (DualAppointmentDTO dualAppointmentDTO : appointmentDTOList) {
            try {
                responseMsgComplete = appointmentService.findOrCreateExportEmptyAppointment(dualAppointmentDTO, userEntity);
                CompletableFuture.allOf(responseMsgComplete).join();
                try {
                    responseMsg = responseMsgComplete.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            } catch (BusinessValidationException e) {
                responseMsg = e.getErrorMessage();
                return new ResponseEntity<String>(responseMsg, HttpStatus.INTERNAL_SERVER_ERROR);
            }

        }
        return new ResponseEntity<String>(responseMsg, HttpStatus.OK);
    }

    @PostMapping("/createBulkAppointmnets")
    public @ResponseBody
    ResponseEntity<String> createBulkAppointmnets(@RequestBody List<BulkAppointmentDTO> bulkAppointmentDTOS) {
        StringBuilder responseMsg = new StringBuilder();
      //  List<BulkAppointmentDTO> appointmentDTOList = validateDTo(bulkAppointmentDTOS, responseMsg);
        Map<String, Map<String, List<BulkAppointmentDTO>>> dateWiseBulkAppointmentDTO = new HashMap<>();
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        //todo: Need to check with Keerthi whether to add the validating dto  condtion in the existing for loop or write a separate method
        //  List<BulkAppointmentDTO> appointmentDTOList = validateDTo(bulkAppointmentDTOS, responseMsg);


        for (BulkAppointmentDTO bulkAppointmentDTO : bulkAppointmentDTOS) {

           // validate the dto
            if (null == bulkAppointmentDTO.getApptDate() || null == bulkAppointmentDTO.getSlot()
                    || null == bulkAppointmentDTO.getTranType1() || null == bulkAppointmentDTO.getContainerNumber1()) {
                responseMsg.append("  Null value   is provided  for container::").append(bulkAppointmentDTO.getContainerNumber1()).append("  or ApptDate::").append(bulkAppointmentDTO.getApptDate())
                        .append(" ").append("or slot::").append(bulkAppointmentDTO.getSlot()).append(" ").append("or tranType::").append(bulkAppointmentDTO.getTranType1()).append("\n");
                continue;
            }
            if ((!"RE".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) && (!"DI".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) &&
                    (!"RM".equalsIgnoreCase(bulkAppointmentDTO.getTranType1()))) {
                responseMsg.append(" In valid Trantype ::").append(bulkAppointmentDTO.getTranType1()).append("is provided  for the container ::").append(bulkAppointmentDTO.getContainerNumber1()).append("\n");
                continue;
            }
            if ("DI".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) {
                if (null == bulkAppointmentDTO.getPin()) {
                    responseMsg.append(" pin number is  null for the container ::" + bulkAppointmentDTO.getContainerNumber1()).append("\n");
                    continue;
                }

            }
            try {
                LocalDateTime.of(LocalDate.parse((CharSequence) bulkAppointmentDTO.getApptDate(), localDateFormat),
                        LocalTime.parse(bulkAppointmentDTO.getSlot()));
            } catch (Exception e) {
                responseMsg.append(" Invalid data format is provided for ApptDate::").append(bulkAppointmentDTO.getApptDate()).append(" ").append("or SlotTime ::").append(bulkAppointmentDTO.getSlot()).append("\n");
                logger.debug("Exception::" + e.getMessage());
                continue;
            }
            //
            if (dateWiseBulkAppointmentDTO.get(bulkAppointmentDTO.getApptDate()) == null) {
                Map<String, List<BulkAppointmentDTO>> bulkAppointmentDTOMap = new HashMap<>();
                List<BulkAppointmentDTO> bulkAppointmentDTOList = new ArrayList<>();
                bulkAppointmentDTOList.add(bulkAppointmentDTO);
                bulkAppointmentDTOMap.put(bulkAppointmentDTO.getSlot(), bulkAppointmentDTOList);
                dateWiseBulkAppointmentDTO.put(bulkAppointmentDTO.getApptDate(), bulkAppointmentDTOMap);
            } else {
                if (dateWiseBulkAppointmentDTO.get(bulkAppointmentDTO.getApptDate()).get(bulkAppointmentDTO.getSlot()) == null) {
                    List<BulkAppointmentDTO> bulkAppointmentDTOList = new ArrayList<>();
                    bulkAppointmentDTOList.add(bulkAppointmentDTO);
                    dateWiseBulkAppointmentDTO.get(bulkAppointmentDTO.getApptDate()).put(bulkAppointmentDTO.getSlot(), bulkAppointmentDTOList);
                } else {
                    dateWiseBulkAppointmentDTO.get(bulkAppointmentDTO.getApptDate()).get(bulkAppointmentDTO.getSlot()).add(bulkAppointmentDTO);
                }
            }
        }

        for (Map.Entry<String, Map<String, List<BulkAppointmentDTO>>> entry : dateWiseBulkAppointmentDTO.entrySet()) {
            String currentDate = entry.getKey();
            Map<String, List<BulkAppointmentDTO>> timeSlotWiseMap = entry.getValue();
            CompletableFuture<String> responseMsgComplete;
            for (Map.Entry<String, List<BulkAppointmentDTO>> timeSlotWiseMapEntry : timeSlotWiseMap.entrySet()) {
                List<BulkAppointmentDTO> bulkAppointmentDTOList = timeSlotWiseMapEntry.getValue();
                try {
                    responseMsgComplete = appointmentService.createBulkAppointment(currentDate, timeSlotWiseMapEntry.getKey(), bulkAppointmentDTOList, userEntity);
                    CompletableFuture.allOf(responseMsgComplete).join();
                    try {
                        responseMsg.append(responseMsgComplete.get()).append("\n");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } catch (BusinessValidationException e) {
                    e.printStackTrace();
                }

            }

        }
        return new ResponseEntity<>(responseMsg.toString(), HttpStatus.OK);
    }

    private List<BulkAppointmentDTO> validateDTo(List<BulkAppointmentDTO> bulkAppointmentDTOS, StringBuilder responseMsg) {
        List<BulkAppointmentDTO> validData = new ArrayList<>();
        for (BulkAppointmentDTO bulkAppointmentDTO : bulkAppointmentDTOS) {
            if (null == bulkAppointmentDTO.getApptDate() || null == bulkAppointmentDTO.getSlot()
                    || null == bulkAppointmentDTO.getTranType1() || null == bulkAppointmentDTO.getContainerNumber1()) {
                responseMsg.append("  null value   is provided  for container::").append(bulkAppointmentDTO.getContainerNumber1()).append("  or ApptDate::").append(bulkAppointmentDTO.getApptDate())
                        .append(" ").append("or slot::").append(bulkAppointmentDTO.getSlot()).append(" ").append("or tranType::").append(bulkAppointmentDTO.getTranType1()).append("\n");
                continue;
            }
            if ((!"RE".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) && (!"DI".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) &&
                    (!"RM".equalsIgnoreCase(bulkAppointmentDTO.getTranType1()))) {
                responseMsg.append(" In valid Trantype ::").append(bulkAppointmentDTO.getTranType1()).append("is provided  for the container ::").append(bulkAppointmentDTO.getContainerNumber1()).append("\n");
                continue;
            }
            if ("DI".equalsIgnoreCase(bulkAppointmentDTO.getTranType1())) {
                if (null == bulkAppointmentDTO.getPin()) {
                    responseMsg.append(" pin number is  null for the container ::" + bulkAppointmentDTO.getPin());
                    continue;
                }

            }
            try {
                LocalDateTime.of(LocalDate.parse((CharSequence) bulkAppointmentDTO.getApptDate(), localDateFormat),
                        LocalTime.parse(bulkAppointmentDTO.getSlot()));
            } catch (Exception e) {
                responseMsg.append(" Invalid data format is provided for ApptDate::").append(bulkAppointmentDTO.getApptDate()).append(" ").append("or SlotTime ::").append(bulkAppointmentDTO.getSlot()).append("\n");
                logger.debug("Exception::" + e.getMessage());
                continue;

            }
            validData.add(bulkAppointmentDTO);

        }
        return validData;

    }

    @PostMapping(path = "disassociateAppt")
    public ResponseEntity<String> disassociateAppointment(@RequestBody AppointmentEntity appointmentEntity) {
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        boolean isDisassociatedSuccessful = appointmentService.disassociateAppointment(appointmentEntity, userEntity);
        if (!isDisassociatedSuccessful) {
            return new ResponseEntity<String>("Disassociated Un Successful..!", HttpStatus.OK);
        }
        return new ResponseEntity<String>("Disassociated sucessfully..!", HttpStatus.OK);
    }

    @PostMapping(path = "apptbulkdel")
    public void deleteTranAppt(@RequestBody List<Long> tranApptList) {
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        for (Long id : tranApptList) {
            appointmentService.deleteTranAppointment(id, userEntity);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    @GetMapping(path = "getDashboardAppt")
    public DashboardResponseDto FetchAllDashboardTransAppointments() {
        return appointmentService.findAllActiveDashboardAppointments();
    }

    @ResponseBody
    @GetMapping(path = "seedAppointmentData")
    public String seedData() {
        return appointmentService.seedAppointmentData();
    }

    @GetMapping(path = "fetchAppointment")
    public List<AppointmentEntity> findAppointment(@RequestParam String searcStringLiteral) {
        UserEntity userEntity = (UserEntity) Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute("user_id", RequestAttributes.SCOPE_REQUEST);
        return appointmentService.findAppointmentUsingSearchLiteral(searcStringLiteral, userEntity);
    }

    @GetMapping(path = "getFutureDaysAppointment")
    public List<AppointmentCountDTO> getFutureDaysAppointment(@RequestParam String finalDate) {
        return appointmentService.getFutureDaysAppointment(finalDate);
    }


    @GetMapping(path = "getImportAppointmentForDual")
    public @ResponseBody
    ResponseEntity<List<AppointmentDTO>> getImportAppointmentForDual() {
        return new ResponseEntity<>(appointmentService.getImportAppointmentForDual(), HttpStatus.OK);
    }


    private static final Logger logger = LoggerFactory.getLogger(AppointmentController.class);
    private static DateTimeFormatter localDateFormat = DateTimeFormatter.ofPattern("yy-MMM-dd");

}
