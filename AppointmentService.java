package com.weserve.appointment.service.services;

import com.weserve.appointment.payment.enums.ValidationSeverityEnum;
import com.weserve.appointment.service.exception.BusinessValidationException;
import com.weserve.appointment.service.hone.HoneService;
import com.weserve.appointment.service.model.dao.*;
import com.weserve.appointment.service.model.dto.*;
import com.weserve.appointment.service.model.entity.*;
import com.weserve.appointment.service.model.enums.AppointmentStateEnum;
import com.weserve.appointment.service.model.enums.TranTypeEnum;
import com.weserve.appointment.service.tos.TOSService;
import com.weserve.appointment.service.util.XmlUtil;
import com.weserve.framework.base.model.dao.UserDao;
import com.weserve.framework.base.model.entity.UserEntity;
import com.weserve.framework.hibernate.common.PersistenceInterface;
import com.weserve.framework.hibernate.model.ITransactionEntity;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by mpraveen@weservetech.com on 10/9/2020.
 */

@Service
@Transactional
public class AppointmentService {

    @Autowired
    UnitDao unitDao;

    @Autowired
    AppointmentDAO appointmentDAO;

    @Autowired
    BookingDAO bookingDAO;

    @Autowired
    AppointmentTimeSlotDao appointmentTimeSlotDao;

    @Autowired
    TOSService tosService;

    @Autowired
    HoneService honeService;

    @Autowired
    TruckVisitApptDAO truckVisitApptDAO;

    @Autowired
    GateDAO gateDAO;

    @Autowired
    TruckEntityDAO truckEntityDAO;

    @Autowired
    BookingService bookingService;

    @Autowired
    UserDao userDao;


    @Nullable
    @Async("taskExecutor")
    public CompletableFuture<UnitEntity> findOrEnquireUnitDetailsAsync(String unitId, String transactionType, boolean forceRequestFromTOS) {
        return CompletableFuture.completedFuture(findOrEnquireUnitDetails(unitId, transactionType, forceRequestFromTOS));
    }

    @Nullable
    public UnitEntity findOrEnquireUnitDetails(String unitId, String transactionType, boolean forceRequestFromTOS) {
        UnitEntity unitEntityLocal = unitDao.findUnitByUnitNbr(unitId);
        if (unitEntityLocal == null || forceRequestFromTOS) {
            List<ITransactionEntity> unitEntityList = null;
            try {
                unitEntityList = tosService.fetchUnit(unitId, transactionType);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if (unitEntityList != null && unitEntityList.size() > 0) {
                for (ITransactionEntity iTransactionEntity : unitEntityList) {
                    UnitEntity unitEntity = (UnitEntity) iTransactionEntity;
                    if (unitEntityLocal != null) {
                        BeanUtils.copyProperties(unitEntity, unitEntityLocal, "transactionObjectId", "watchListEntitySet");
                        PersistenceInterface.getInstance().saveOrUpdate(unitEntityLocal);
                        PersistenceInterface.getInstance().flush();
                        PersistenceInterface.getInstance().evict(unitEntityLocal);
                        return unitEntityLocal;
                    } else {
                        PersistenceInterface.getInstance().saveOrUpdate(unitEntity);
                        PersistenceInterface.getInstance().flush();
                        PersistenceInterface.getInstance().evict(unitEntity);
                        return unitEntity;
                    }
                }
            }
        } else {
            return unitEntityLocal;
        }
        return null;
    }

    @Async
    public CompletableFuture<List<AppointmentTimeSlotEntity>> findOrEnquireTOSAppointmentTimeSlotAsync(String date, String transactionType) {
        List<AppointmentTimeSlotEntity> appointmentTimeSlotEntites = new ArrayList<>(Objects.requireNonNull(findOrEnquireAppointmentTimeSlot(date, transactionType)));
        return CompletableFuture.completedFuture(appointmentTimeSlotEntites);
    }

    @Async
    public CompletableFuture<List<AppointmentTimeSlotEntity>> findOrEnquireLocalAppointmentTimeSlotAsync(String unitId, String transactionType) {
        List<AppointmentTimeSlotEntity> appointmentTimeSlotEntites = null;
        if (transactionType.equalsIgnoreCase("RE")) {
            transactionType = "Receive Full";
            appointmentTimeSlotEntites = new ArrayList<AppointmentTimeSlotEntity>(Objects.requireNonNull(findLocalAppointmentTimeSlot(transactionType)));
        } else {
            transactionType = "Receive Empty";
            UnitEntity unitEntity = unitDao.findUnitByUnitNbr(unitId);
            if (unitEntity != null) {
                appointmentTimeSlotEntites = new ArrayList<AppointmentTimeSlotEntity>(Objects.requireNonNull(findLocalAppointmentTimeSlotByLineOP(unitEntity.getLineOp(), transactionType, unitEntity.getTypeISO())));
            }

        }
        return CompletableFuture.completedFuture(appointmentTimeSlotEntites);
    }

    private List<AppointmentTimeSlotEntity> findLocalAppointmentTimeSlotByLineOP(String lineOp, String transactionType, String containerISO) {
        return appointmentTimeSlotDao.findTimeSlotByTransactionTypeLineOp(lineOp, transactionType, containerISO);
    }

    private List<AppointmentTimeSlotEntity> findLocalAppointmentTimeSlot(String transactionType) {
        return appointmentTimeSlotDao.findTimeSlotByTransactionType(transactionType, null);
    }

    @Nullable
    public List<AppointmentTimeSlotEntity> findOrEnquireAppointmentTimeSlot(String date, String tranType) {
      /*  List appointmentTimeSlotEntityList = appointmentTimeSlotDao.findTimeSlotByDate(date);
        logger.debug("Inside the appointment service :: findOrEnquireAppointmentTimeSlot ::  " + appointmentTimeSlotEntityList);
        if (appointmentTimeSlotEntityList != null && appointmentTimeSlotEntityList.size() > 0) {
            return appointmentTimeSlotEntityList;
        } else {*/
        List<AppointmentTimeSlotEntity> appointmentTimeSlotEntityList = new ArrayList<>();
        String derivedTranType = tranType;
        switch (tranType) {
            case "DI":
                derivedTranType = "DF";
                break;
            case "RE":
                derivedTranType = "RF";
                break;
        }
        List<ITransactionEntity> apptTimeSlotEntityList = tosService.fetchAppointmentTimeSlot(date, derivedTranType, appointmentTimeSlotDao);
        if (apptTimeSlotEntityList.size() > 5) {
            for (int i = apptTimeSlotEntityList.size() - 1; i >= apptTimeSlotEntityList.size() - 5; i--) {
                AppointmentTimeSlotEntity timeSlotEntity = (AppointmentTimeSlotEntity) apptTimeSlotEntityList.get(i);
                PersistenceInterface.getInstance().saveOrUpdate(timeSlotEntity);

                appointmentTimeSlotEntityList.add(timeSlotEntity);
                PersistenceInterface.getInstance().flush();
                PersistenceInterface.getInstance().evict(timeSlotEntity);
                logger.debug("Printing Log by slf4j n4_external {}", timeSlotEntity);
            }
        } else {
            for (ITransactionEntity iTransactionEntity : apptTimeSlotEntityList) {
                AppointmentTimeSlotEntity timeSlotEntity = (AppointmentTimeSlotEntity) iTransactionEntity;
                PersistenceInterface.getInstance().saveOrUpdate(timeSlotEntity);

                appointmentTimeSlotEntityList.add(timeSlotEntity);
                PersistenceInterface.getInstance().flush();
                PersistenceInterface.getInstance().evict(timeSlotEntity);
                logger.debug("Printing Log by slf4j n4_external {}", timeSlotEntity);
            }
        }
        return appointmentTimeSlotEntityList;
        // }
    }

    public List<AppointmentTimeSlotEntity> getHoneApptTimeSlots(List<AppointmentTimeSlotEntity> apptslotlist) {
        List<AppointmentTimeSlotEntity> appointmentTimeSlotEntityList = appointmentTimeSlotEntityList = new ArrayList<>();
        for (AppointmentTimeSlotEntity appointmentTimeSlotEntity : apptslotlist) {
            appointmentTimeSlotEntityList = appointmentTimeSlotDao.getHoneAppt(appointmentTimeSlotEntity.getUnitNbr(), appointmentTimeSlotEntity.getTransType(), appointmentTimeSlotEntity.getGateId());
        }
        return appointmentTimeSlotEntityList;
    }

    public CompletableFuture<List<AppointmentTimeSlotEntity>> findOrEnquireHoneAppointmentTimeSlotAsync(String unitId, String transactionType) throws BusinessValidationException {
        List<AppointmentTimeSlotEntity> appointmentTimeSlotEntites = new ArrayList<>(Objects.requireNonNull(findOrEnquireHoneAppointmentTimeSlot(unitId, transactionType)));
        return CompletableFuture.completedFuture(appointmentTimeSlotEntites);
    }

    //Hone Integration
    @Nullable
    public List<AppointmentTimeSlotEntity> findOrEnquireHoneAppointmentTimeSlot(String unitId, String transactionType) throws BusinessValidationException {
        logger.debug("**findOrEnquireHoneAppointmentTimeSlot**");
        UnitEntity unitEntity = unitDao.findUnitByUnitNbr(unitId);
        boolean isNewApptCreation = true;
        if (unitEntity != null) {
            AppointmentEntity appointmentEntity = appointmentDAO.findActiveApptByUnitId(unitEntity.getTransactionObjectId());
            if (appointmentEntity != null && ((appointmentEntity.getHoneStatus() == null && appointmentEntity.getApptExtNbr() != null)
                    || AppointmentStateEnum.CONFIRM.equals(appointmentEntity.getHoneStatus()))) {
                isNewApptCreation = false;
            }
        }
        return honeService.fetchHONEAPIAppointmentTimeSlot(unitId, transactionType, isNewApptCreation);
        //return honeService.fetchMCAAPIAppointmentTimeSlot(unitId, transactionType, "MAIN");
    }

    @Nullable
    public List<AppointmentEntity> findAppointmentForSpecificSlot(Long timeSlotId) {
        return appointmentDAO.findByTimeSlot(timeSlotId);
    }

    @Nullable
    public AppointmentListDto findAllActiveAppointments(Pageable pageable, String sortOrder, String sortColumn, List<FilterDto> filterDtoList) {
        return appointmentDAO.findAll(pageable, sortOrder, sortColumn, filterDtoList);
    }

    @Nullable
    public List<AppointmentEntity> findAllDeletedAppointments() {
        return appointmentDAO.findAllDeletedAppt();
    }

    public boolean disassociateAppointment(AppointmentEntity appointmentEntity, UserEntity userEntity) {
        if (appointmentEntity != null) {
            appointmentEntity = appointmentDAO.findActiveApptByUnitId(appointmentEntity.getApptUnit().getTransactionObjectId());
            TruckVisitAppointment truckVisitAppointment = appointmentEntity.getGapptTruckVisitAppt();
            if (appointmentEntity.isDualAppointmentUsed()) {
                appointmentEntity.setDualAppointmentUsed(false);
            }

            if (truckVisitAppointment.getAppointmentEntityList() != null && truckVisitAppointment.getAppointmentEntityList().size() == 2) {
                for (AppointmentEntity existigAppointmentEntity : truckVisitAppointment.getAppointmentEntityList()) {
                    if (!appointmentEntity.equals(existigAppointmentEntity) && appointmentEntity.isDualAppointmentUsed()) {
                        appointmentEntity.setDualAppointmentUsed(false);
                    }
                }
            }

            truckVisitAppointment.disassociateTranAppointment(appointmentEntity, false);
            PersistenceInterface.getInstance().saveOrUpdate(truckVisitAppointment);
            tosService.interfaceDetailsToTOS(appointmentEntity, userEntity, "DISASSOCIATE");
            return true;
        }
        return false;
    }

    @Nullable
    public List<AppointmentEntity> findApptByTruckVisitAppt(TruckVisitAppointment truckVisit, UserEntity userEntity) {
        return appointmentDAO.findApptByTruckVisit(truckVisit, userEntity);
    }

    @Nullable
    public AppointmentEntity findByUnit(UnitEntity unitEntity) {
        return appointmentDAO.findApptByUnit(unitEntity, INACTIVE_APPT_STATUS);
    }

    @Nullable
    public AppointmentEntity findByUnitId(Serializable unitEntityId) {
        AppointmentEntity appointmentEntity = appointmentDAO.findActiveApptByUnitId(unitEntityId);
        if (appointmentEntity != null && !appointmentEntity.isDelete() && ACTIVE_APPT_STATUS.contains(appointmentEntity.getApptStatus())) {
            return appointmentEntity;
        }
        return null;
    }

    public void deleteTranAppointment(Long id, UserEntity userEntity) {
        if (id != null) {
            AppointmentEntity appointment = (AppointmentEntity) PersistenceInterface.getInstance().findById(AppointmentEntity.class, id);
            TruckVisitAppointment truckVisitAppointment = appointment != null ? appointment.getGapptTruckVisitAppt() : null;
            if (appointment != null) {
                tosService.interfaceDetailsToTOS(appointment, userEntity, "DELETE");
                if (truckVisitAppointment != null) {
                    truckVisitAppointment = truckVisitAppointment.disassociateTranAppointment(appointment, true);
                    List<AppointmentEntity> appointmentEntityList1 = truckVisitAppointment != null ? List.copyOf(truckVisitAppointment.getAppointmentEntityList()) : null;
                    if (appointmentEntityList1 != null) {
                        if (appointmentEntityList1.size() == 0) {
                            truckVisitAppointment.setIsDelete(true);
                            truckVisitAppointment.setState(AppointmentStateEnum.CANCEL);
                        } else if (appointmentEntityList1.size() == 1 && TranTypeEnum.DI.equals(appointmentEntityList1.get(0).getApptTranType())) {
                            appointmentEntityList1.get(0).setDualAppointmentUsed(false);
                        }

                    }
                    PersistenceInterface.getInstance().saveOrUpdate(truckVisitAppointment);
                } else {
                    appointmentDAO.deleteById(id);
                }
                PersistenceInterface.getInstance().flush();
            }
        }
    }

    @Async("taskExecutor")
    public CompletableFuture<String> createBulkAppointment(String date, String timeSlot, List<BulkAppointmentDTO> bulkAppointmentDTOList, UserEntity userEntity) throws BusinessValidationException {
        StringBuilder responseMsg = new StringBuilder();
        LocalDate localDate = (LocalDate) LocalDate.parse((CharSequence) date, localDateFormat);
        LocalTime localTime = LocalTime.parse(timeSlot);
        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);

        CompletableFuture<String> responseMsgComplete = null;
        for (BulkAppointmentDTO bulkAppointmentDTO : bulkAppointmentDTOList) {
            AppointmentEntity appointmentEntity = new AppointmentEntity();
            UnitEntity apptUnit = findOrEnquireUnitDetails(bulkAppointmentDTO.getContainerNumber1(), bulkAppointmentDTO.getTranType1(), false);
            logger.debug("unit  ::" + apptUnit);

            if (apptUnit == null) {
                responseMsg.append("Unable to find Container Details in TOS ").append(bulkAppointmentDTO.getContainerNumber1()).append("\n");
                logger.debug("apptUnit  is null::" + apptUnit);
                continue;
            }
            appointmentEntity.setApptUnit(apptUnit);
           // appointmentEntity.setApptUnit(findOrEnquireUnitDetails(bulkAppointmentDTO.getContainerNumber1(), bulkAppointmentDTO.getTranType1(), false));
            List<AppointmentTimeSlotEntity> appointmentTimeSlotEntityList=null;
            if (bulkAppointmentDTO.getTranType1().equalsIgnoreCase("DI")) {
                try {
                    appointmentTimeSlotEntityList = findOrEnquireHoneAppointmentTimeSlot(bulkAppointmentDTO.getContainerNumber1(), bulkAppointmentDTO.getTranType1());
                } catch(Exception e){
                    logger.debug( " hone Time slot exception "+e.getMessage());
                }
                } else if (bulkAppointmentDTO.getTranType1().equalsIgnoreCase("RE")) {
                appointmentTimeSlotEntityList = appointmentTimeSlotDao.findTimeSlotByTransactionType("Receive Full", XmlUtil.convertStringToDateOnly("20" + date));
            } else {
                appointmentTimeSlotEntityList = appointmentTimeSlotDao.
                        findTimeSlotByTransactionTypeLineOp(appointmentEntity.getApptUnit().getLineOp(), "Receive Empty", null);

                //appointmentTimeSlotEntityList = findOrEnquireAppointmentTimeSlot(date, bulkAppointmentDTO.getTranType1());
            }
            if (appointmentTimeSlotEntityList != null && appointmentTimeSlotEntityList.size() > 0) {
                for (AppointmentTimeSlotEntity appointmentTimeSlotEntity1 : appointmentTimeSlotEntityList) {
                    if (appointmentTimeSlotEntity1.getStartTime().isEqual(localDateTime)) {
                        appointmentEntity.setGapptTimeSlot(appointmentTimeSlotEntity1);
                        break;
                    }
                }
                /**
                 * IF there is no mathcing time slot available for the selected date that is received from the file, then select the first available time slot
                 * that is recieved.
                 */

                if (appointmentEntity.getGapptTimeSlot() == null) {
                    appointmentEntity.setGapptTimeSlot(appointmentTimeSlotEntityList.get(0));
                }
                if (appointmentEntity.getGapptTimeSlot() != null) {
                    appointmentEntity.setApptDate(appointmentEntity.getGapptTimeSlot().getStartTime().toLocalDate());
                }
                appointmentEntity.setGate(gateDAO.findGateByGateId("MAIN"));
                appointmentEntity.setGapptTruck(truckEntityDAO.findTruckByTruckLicense(bulkAppointmentDTO.getTruckLicense()));
                appointmentEntity.setApptTranType(TranTypeEnum.valueOf(bulkAppointmentDTO.getTranType1()));
                if (StringUtils.isNotEmpty(bulkAppointmentDTO.getPin())) {
                    appointmentEntity.setGapptPinNbr(bulkAppointmentDTO.getPin());
                }
                if (bulkAppointmentDTO.getPin() != null) {
                    appointmentEntity.setGapptPinNbr(bulkAppointmentDTO.getPin());
                }
                if (bulkAppointmentDTO.getBookingNumber() != null) {
                    String bookingType = "BOOKING";
                    switch (bulkAppointmentDTO.getTranType1()) {
                        case "RE":
                            bookingType = "BOOKING";
                            break;
                        case "RM":
                            bookingType = "ERO";
                            break;
                        case "DM":
                            bookingType = "EDO";
                            break;
                    }
                    BookingEntity bookingEntity = bookingService.findOrEnquireBookingDetails(bulkAppointmentDTO.getBookingNumber(), bookingType,
                            appointmentEntity.getApptUnit().getUnitNbr(), appointmentEntity.getApptUnit().getLineOp());
                    if (bookingEntity != null) {
                        appointmentEntity.setGapptBooking(bookingEntity);
                    }
                }

                responseMsgComplete = findOrCreateAppointment(appointmentEntity, userEntity);
                CompletableFuture.allOf(responseMsgComplete).join();
                try {
                    responseMsg.append("Unit :: ").append(bulkAppointmentDTO.getContainerNumber1()).append(" ").append(responseMsgComplete.get()).append("\n");
                } catch (InterruptedException e) {
                    responseMsg.append("Unit :: ").append(bulkAppointmentDTO.getContainerNumber1()).append("Error Processing Appointment Creation").append("\n");
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    responseMsg.append("Unit :: ").append(bulkAppointmentDTO.getContainerNumber1()).append("Error Processing Appointment Creation").append("\n");
                    e.printStackTrace();
                }
            } else {
                responseMsg.append(bulkAppointmentDTO.getContainerNumber1()).append(" :: ").append("No time slots available.").append("\n");
            }
        }
        return CompletableFuture.completedFuture(responseMsg.toString());
    }

    @Async
    public CompletableFuture<String> findOrCreateExportEmptyAppointment(DualAppointmentDTO dualAppointmentDTO, UserEntity userEntity) throws BusinessValidationException {
        logger.debug("Create Export or Empty Appointment Service :: " + dualAppointmentDTO);
        TruckVisitAppointment truckVisitAppointment = null;
        String responseMsg;
        if (dualAppointmentDTO.getTvApptKey() != null) {
            truckVisitAppointment = (TruckVisitAppointment) PersistenceInterface.getInstance().findById(TruckVisitAppointment.class, dualAppointmentDTO.getTvApptKey());
        }
        if (truckVisitAppointment == null) {
            truckVisitAppointment = new TruckVisitAppointment();
        }
        logger.debug("Create Export or Empty Appointment Service :: TruckVisitAppt ::  " + truckVisitAppointment);
        List<AppointmentDTO> appointmentDTOList = dualAppointmentDTO.getAppointmentDTOList();
        List<AppointmentEntity> interfaceAppointmentList = new ArrayList<>();
        for (AppointmentDTO appointmentDTO : appointmentDTOList) {
            AppointmentEntity appointmentEntity = null;
            if (appointmentDTO.getApptKey() != null) {
                appointmentEntity = appointmentDAO.findActiveApptByUnitId(appointmentDTO.getApptKey());
                logger.debug("Create Export or Empty Appointment Service :: Existing Appointment ::  " + appointmentEntity);
                if (appointmentEntity != null) {
                    if (!appointmentEntity.getUser().equals(userEntity)
                            && ACTIVE_APPT_STATUS.contains(appointmentEntity.getApptStatus()) && !appointmentEntity.isDelete()) {
                        responseMsg = "Active Appointment already created for the Container " + appointmentEntity.getApptUnit().getUnitNbr() + "by " + appointmentEntity.getUser().getUserId() + ". Update cannot be done.";
                        return CompletableFuture.completedFuture(responseMsg);
                    }
                    if (INACTIVE_APPT_STATUS.contains(appointmentEntity.getApptStatus())) {
                        if (appointmentDTOList.size() > 1) {
                            appointmentEntity.setDualAppointmentUsed(true);
                        }
                        updateValuesFromDTOToEntity(appointmentEntity, appointmentDTO, truckVisitAppointment, true, userEntity);
                        interfaceAppointmentList.add(appointmentEntity);
                    } else {
                        boolean isNewAppointment = true;
                        if (!AppointmentStateEnum.CANCEL.equals(appointmentEntity.getApptStatus())) {
                            if (appointmentEntity.getGapptTruckVisitAppt() != null && dualAppointmentDTO.getTvApptKey() == null) {
                                truckVisitAppointment = appointmentEntity.getGapptTruckVisitAppt();
                            }
                            isNewAppointment = false;
                        }

                        if (appointmentDTOList.size() > 1) {
                            appointmentEntity.setDualAppointmentUsed(true);
                            if (appointmentDTO.getUnitNbr() != null && appointmentEntity.getGapptTruckVisitAppt() != null) {
                                appointmentEntity.getGapptTruckVisitAppt().disassociateTranAppointment(appointmentEntity, false);
                            }
                        }
                        if (appointmentDTO.getUnitNbr() != null) {
                            updateValuesFromDTOToEntity(appointmentEntity, appointmentDTO, truckVisitAppointment, isNewAppointment, userEntity);
                            interfaceAppointmentList.add(appointmentEntity);
                        }
                    }
                }
                if (appointmentEntity == null) {
                    logger.debug("Create Export or Empty Appointment Service :: Creating new Appointment for ::  " + appointmentDTO.getUnitNbr());
                    appointmentEntity = new AppointmentEntity();
                    if (appointmentDTOList.size() > 1) {
                        appointmentEntity.setDualAppointmentUsed(true);
                    }
                    updateValuesFromDTOToEntity(appointmentEntity, appointmentDTO, truckVisitAppointment, true, userEntity);
                    interfaceAppointmentList.add(appointmentEntity);
                }/*else if (appointmentEntity.getApptTranType().equals(AppointmentStateEnum.REJECTED)) {
                    if (appointmentDTOList.size() > 1) {
                        appointmentEntity.setDualAppointmentUsed(true);
                    }
                    logger.debug("Create Export or Empty Appointment Service :: Creating new Appointment for ::  " + appointmentDTO.getUnitNbr());
                    updateValuesFromDTOToEntity(appointmentEntity, appointmentDTO, truckVisitAppointment, true, userEntity);
                    interfaceAppointmentList.add(appointmentEntity);
                }*/
            }
        }
        logger.debug("Create Export or Empty Appointment Service :: After processing the appointment TruckVisitAppt ::  " + truckVisitAppointment);
        PersistenceInterface.getInstance().saveOrUpdate(truckVisitAppointment);
        for (AppointmentEntity appointmentEntity : interfaceAppointmentList) {
            logger.debug("Create Export or Empty Appointment Service :: Interface appointment details to TOS ::  " + appointmentEntity);
            tosService.interfaceDetailsToTOS(appointmentEntity, userEntity, "CREATE");
        }
        return CompletableFuture.completedFuture("Appt and Tv updated successfully");
    }

    private void updateValuesFromDTOToEntity(AppointmentEntity appointmentEntity, AppointmentDTO appointmentDTO,
                                             TruckVisitAppointment truckVisitAppointment, boolean isNewAppointment, UserEntity userEntity) throws BusinessValidationException {
        if (isNewAppointment) {
            appointmentEntity.setApptUnit(unitDao.findUnitById(appointmentDTO.getApptKey()));
            appointmentEntity.setGate((GateEntity) PersistenceInterface.getInstance().findById(GateEntity.class, appointmentDTO.getGateKey()));
            truckVisitAppointment.setGate(appointmentEntity.getGate());
            appointmentEntity.setApptStatus(AppointmentStateEnum.CREATED);
            truckVisitAppointment.setState(AppointmentStateEnum.CREATED);
            if (truckVisitAppointment.getApptSlot() != null) {
                appointmentEntity.setGapptTimeSlot(truckVisitAppointment.getApptSlot());
            } else {
                AppointmentTimeSlotEntity appointmentTimeSlotEntity = (AppointmentTimeSlotEntity) PersistenceInterface.getInstance().
                        findById(AppointmentTimeSlotEntity.class, appointmentDTO.getTimeSlotKey());
                if (appointmentEntity.getApptTranType() != null
                        && !appointmentEntity.getApptTranType().name().equalsIgnoreCase(appointmentTimeSlotEntity.getTransType())
                        && appointmentEntity.isDualAppointmentUsed()) {
                    appointmentTimeSlotEntity = appointmentTimeSlotDao.findApptSlotByDateAndTimeSlot(appointmentTimeSlotEntity.getDate(),
                            appointmentTimeSlotEntity.getStartTime(), TranTypeEnum.DMT.name());
                    if (appointmentTimeSlotEntity != null) {
                        appointmentEntity.setGapptTimeSlot((AppointmentTimeSlotEntity) PersistenceInterface.getInstance().
                                findById(AppointmentTimeSlotEntity.class, appointmentDTO.getTimeSlotKey()));
                        truckVisitAppointment.setApptSlot(appointmentEntity.getGapptTimeSlot());
                    } else {
                        throw new BusinessValidationException("Error creating Appointment as matching slot not available", ValidationSeverityEnum.ERROR);
                    }
                } else {
                    appointmentEntity.setGapptTimeSlot(appointmentTimeSlotEntity);
                    truckVisitAppointment.setApptSlot(appointmentEntity.getGapptTimeSlot());
                }
            }
            if (truckVisitAppointment.getDate() != null) {
                appointmentEntity.setApptDate(truckVisitAppointment.getDate());
            } else {
                truckVisitAppointment.setDate(XmlUtil.convertDateToLocalDate(appointmentEntity.getGapptTimeSlot().getDate()));
                appointmentEntity.setApptDate(truckVisitAppointment.getDate());
            }
            appointmentEntity.setApptTranType(appointmentDTO.getApptTranType());
            appointmentEntity.setApptTruckingCompany(userEntity.getUserCompany());
            appointmentEntity.setUser(userEntity);
            truckVisitAppointment.setUser(appointmentEntity.getUser());
            truckVisitAppointment.associateTranAppointment(appointmentEntity);
        } else {
            appointmentEntity.setApptStatus(AppointmentStateEnum.UPDATED);
            truckVisitAppointment.setState(AppointmentStateEnum.UPDATED);
            if (appointmentDTO.getTimeSlotKey() != null) {
                appointmentEntity.setGapptTimeSlot((AppointmentTimeSlotEntity) PersistenceInterface.getInstance().
                        findById(AppointmentTimeSlotEntity.class, appointmentDTO.getTimeSlotKey()));
            }
            truckVisitAppointment.setApptSlot(appointmentEntity.getGapptTimeSlot());
            truckVisitAppointment.setDate(XmlUtil.convertDateToLocalDate(appointmentEntity.getGapptTimeSlot().getDate()));
            if (appointmentDTO.getApptDate() != null) {
                appointmentEntity.setApptDate(appointmentDTO.getApptDate());
            }
            if (truckVisitAppointment.getGate() == null && appointmentEntity.getGate() != null) {
                truckVisitAppointment.setGate(appointmentEntity.getGate());
            }
            appointmentEntity.setDualAppointmentUsed(true);
            truckVisitAppointment.associateTranAppointment(appointmentEntity);
        }
        //appointmentEntity.setDelete(false);
        if (appointmentDTO.getTruckKey() != null) {
            appointmentEntity.setGapptTruck((TruckEntity) PersistenceInterface.getInstance().findById(TruckEntity.class, appointmentDTO.getTruckKey()));
        } else {
            appointmentEntity.setGapptTruck(null);
        }
        if (appointmentDTO.getBookingKey() != null) {
            appointmentEntity.setGapptBooking((BookingEntity) PersistenceInterface.getInstance().
                    findById(BookingEntity.class, appointmentDTO.getBookingKey()));
        }
        if (appointmentDTO.getApptSealNbr() != null) {
            logger.debug("Seal::" + appointmentDTO.getApptSealNbr());
            appointmentEntity.setApptSealNbr1(appointmentDTO.getApptSealNbr());
        }
    }

    @Async("taskExecutor")
    public CompletableFuture<String> findOrCreateAppointment(AppointmentEntity appointmentEntity, UserEntity userEntity) {
        String responseMsg = null;
        String operation = null;
        TruckVisitAppointment truckVisitAppointment = null;
        //Comparing PIN from N4 and PIN Given during Appt creation, if mismatch return
        String inUnitPin = appointmentEntity.getApptUnit() != null ? appointmentEntity.getApptUnit().getPIN() : null;
        if (TranTypeEnum.DI.equals(appointmentEntity.getApptTranType())) {
            String inApptPin = appointmentEntity.getGapptPinNbr();
            logger.debug("appointment inApptTran::" + appointmentEntity.getApptTranType());
            if (inUnitPin == null) {
                responseMsg = "Pin Number Unavailable in N4";
            }
            if (!(inApptPin.equalsIgnoreCase(inUnitPin))) {
                logger.debug("appointment inApptTran Mismatch::" + inApptPin + " " + inUnitPin);
                responseMsg = "Pin Number Mismatch";
            }
            if (responseMsg != null) {
                return CompletableFuture.completedFuture(responseMsg);
            }
        }

        AppointmentEntity existingAppointment = null;
        boolean isinactive = false;
        if (appointmentEntity.getApptUnit() != null) {
            existingAppointment = appointmentDAO.findActiveApptByUnitId(appointmentEntity.getApptUnit().getTransactionObjectId());
            if (existingAppointment != null && !existingAppointment.getUser().equals(userEntity)
                    && ACTIVE_APPT_STATUS.contains(existingAppointment.getApptStatus()) && !existingAppointment.isDelete()) {
                responseMsg = "Active Appointment already created for the Container " + existingAppointment.getApptUnit().getUnitNbr() + "by " + existingAppointment.getUser().getUserId() + ". Update cannot be done.";
                return CompletableFuture.completedFuture(responseMsg);
                /*} else if (!existingAppointment.getUser().equals(userEntity) && (!existingAppointment.isDelete())) {
                    responseMsg = "Active Appointment already created for the same unit by user " + existingAppointment.getUser().getUserId();
                    return CompletableFuture.completedFuture(responseMsg);
                }*/
            }
            if (existingAppointment != null && INACTIVE_APPT_STATUS.contains(existingAppointment.getApptStatus())) {
                isinactive = true;
            }
        }
        logger.debug("T" + existingAppointment);
        if (appointmentEntity.getGapptTimeSlot() != null && PersistenceInterface.getInstance().findById(AppointmentTimeSlotEntity.class, appointmentEntity.getGapptTimeSlot().getTransactionObjectId()) == null) {
            appointmentEntity.setGapptTimeSlot(findOrEnquireAppointmentTimeSlot(XmlUtil.convertLocalDateToString(appointmentEntity.getApptDate()), appointmentEntity.getApptTranType().name()).get(0));
        }
        if (userEntity != null) {
            appointmentEntity.setUser(userEntity);
        }
        if (appointmentEntity.getApptTruckingCompany() == null) {
            appointmentEntity.setApptTruckingCompany(userEntity.getUserCompany());
        }

        if (TranTypeEnum.DM.equals(appointmentEntity.getApptTranType()) || TranTypeEnum.RM.equals(appointmentEntity.getApptTranType())) {
            if (appointmentEntity.getApptUnit() == null) {
                appointmentEntity.setApptUnit(new UnitEntity());
            }
        }

        AppointmentStateEnum derivedAppointmentStatus = AppointmentStateEnum.CREATED;
        if (existingAppointment != null && !isinactive) {
            operation = "CREATE";
            responseMsg = "Appt and Tv updated successfully";
            truckVisitAppointment = existingAppointment.getGapptTruckVisitAppt();
            if (truckVisitAppointment == null) {
                truckVisitAppointment = new TruckVisitAppointment();
            }
            derivedAppointmentStatus = AppointmentStateEnum.UPDATED;
            BeanUtils.copyProperties(appointmentEntity, existingAppointment, "transactionObjectId", "apptNbr", "apptExtNbr", "gapptTruckVisitAppt", "delete", "id");
            if (appointmentEntity.getApptDate() == null || existingAppointment.getApptDate() == null) {
                appointmentEntity.setApptDate(existingAppointment.getGapptTimeSlot().getStartTime().toLocalDate());
            }
        } else {
            operation = "CREATE";
            responseMsg = "Appt and Tv created successfully";
            if (!isinactive) {
                existingAppointment = new AppointmentEntity();
            }
            truckVisitAppointment = new TruckVisitAppointment();
            //UnitEntity unitEntity = new UnitEntity();
            BeanUtils.copyProperties(appointmentEntity, existingAppointment, "transactionObjectId", "delete", "id");
           /* BeanUtils.copyProperties(appointmentEntity.getApptUnit(), unitEntity, "transactionObjectId");
            existingAppointment.setApptUnit(unitEntity);*/
            if (existingAppointment.getGapptTimeSlot() != null) {
                appointmentEntity.setApptDate(existingAppointment.getGapptTimeSlot().getStartTime().toLocalDate());
            }
        }
        appointmentEntity.setApptStatus(derivedAppointmentStatus);
        if (TranTypeEnum.DI.equals(appointmentEntity.getApptTranType())) {
            appointmentEntity.setHoneStatus(derivedAppointmentStatus);
        }
        truckVisitAppointment.setState(derivedAppointmentStatus);
        truckVisitAppointment.setTruck(appointmentEntity.getGapptTruck());
        truckVisitAppointment.setGate(appointmentEntity.getGate());
        truckVisitAppointment.setDate(appointmentEntity.getApptDate());
        truckVisitAppointment.setApptSlot(appointmentEntity.getGapptTimeSlot());
        truckVisitAppointment.setUser(appointmentEntity.getUser());
        truckVisitAppointment.associateTranAppointment(existingAppointment);
        truckVisitAppointment.setTruckingCompany(appointmentEntity.getApptTruckingCompany());
        PersistenceInterface.getInstance().saveOrUpdate(truckVisitAppointment);
        tosService.interfaceDetailsToTOS(existingAppointment, userEntity, operation);
        return CompletableFuture.completedFuture(responseMsg);
    }

    @Nullable
    public DashboardResponseDto findAllActiveDashboardAppointments() {
        Long createdCount = Long.valueOf(0);
        Long usedCount = Long.valueOf(0);
        Long expiredCount = Long.valueOf(0);
        Long invalidCount = Long.valueOf(0);
        Long emptyCount = Long.valueOf(0);
        Long exportCount = Long.valueOf(0);
        Long importCount = Long.valueOf(0);
        CardResponseDto cardResponseDto = new CardResponseDto();
        ApptCreatedTodayDto apptCreatedTodayDto = new ApptCreatedTodayDto();
        DashboardResponseDto dashboardResponseDto = new DashboardResponseDto();
        AppointmentListDto appointmentListDto = appointmentDAO.findAll(null, null, null, null);
        List<AppointmentEntity> appointmentEntityList = appointmentListDto.getAppointmentEntityList();
        for (AppointmentEntity appointmentEntity : appointmentEntityList) {
            if (appointmentEntity.getApptStatus() != null) {
                switch (appointmentEntity.getApptStatus()) {
                    case CREATED:
                    case ACCEPTED:
                    case UPDATED:
                        createdCount++;
                        break;
                    case EXPIRED:
                        expiredCount++;
                        break;
                    case USED:
                    case USEDLATE:
                        usedCount++;
                        break;
                    default:
                        invalidCount++;
                        break;
                }
            } else {
                invalidCount++;
            }
            logger.debug("Appointment Date " + appointmentEntity.getApptDate());
            logger.debug("Today's Date " + new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
            if (appointmentEntity.getApptDate() != null && appointmentEntity.getApptDate().compareTo(new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDate()) == 0) {
                switch (appointmentEntity.getApptTranType()) {
                    case DI:
                        importCount++;
                        break;
                    case RE:
                        exportCount++;
                        break;
                    default:
                        emptyCount++;
                        break;
                }
            }
        }
        cardResponseDto.setCreatedCount(createdCount);
        cardResponseDto.setExpiredCount(expiredCount);
        cardResponseDto.setUsedCount(usedCount);
        cardResponseDto.setInvalidCount(invalidCount);
        apptCreatedTodayDto.setImportCount(importCount);
        apptCreatedTodayDto.setExportCount(exportCount);
        apptCreatedTodayDto.setEmptyCount(emptyCount);
        dashboardResponseDto.setApptCreatedDto(apptCreatedTodayDto);

        dashboardResponseDto.setCardResponse(cardResponseDto);
        return dashboardResponseDto;
    }

    public String seedAppointmentData() {
        UserEntity userEntity = userDao.findByKeycloakPrincipalId("71376487-5e1f-4012-b73e-7d9717e206ac");
        RequestContextHolder.currentRequestAttributes().setAttribute("user_id", userEntity, RequestAttributes.SCOPE_REQUEST);

        Pageable pageable = PageRequest.of(1, 20, Sort.by(AppointmentEntity_.APPT_NBR).descending());
        AppointmentListDto appointmentListDto = appointmentDAO.findAll(pageable, "DESC", null, null);
        UnitEntity existingUnitEnity = null;
        AppointmentEntity existingAppointment = null;
        if (appointmentListDto != null) {
            List<AppointmentEntity> appointmentEntityList = appointmentListDto.getAppointmentEntityList();
            if (appointmentEntityList != null && appointmentEntityList.size() > 0) {
                existingUnitEnity = appointmentEntityList.get(0).getApptUnit();
                existingAppointment = appointmentEntityList.get(0);
            }
        }

        for (int i = 0; i < 10; i++) {
            StringBuilder stringBuilder = new StringBuilder("TESTUNIT");
            AppointmentEntity appointmentEntity = new AppointmentEntity();
            UnitEntity newUnitEntity = new UnitEntity();
            BeanUtils.copyProperties(existingUnitEnity, newUnitEntity, "transactionObjectId", "watchListEntitySet", "unitNbr");
            BeanUtils.copyProperties(existingAppointment, appointmentEntity, "transactionObjectId", "apptUnit", "apptNbr", "apptExtNbr", "gapptTruckVisitAppt", "delete", "id");
            newUnitEntity.setUnitNbr(stringBuilder.append(String.valueOf(i)).toString());
            appointmentEntity.setDelete(false);
            appointmentEntity.setApptUnit(newUnitEntity);
            TruckVisitAppointment truckVisitAppointment = new TruckVisitAppointment();
            BeanUtils.copyProperties(existingAppointment.getGapptTruckVisitAppt(), truckVisitAppointment, "appointmentEntityList", "transactionObjectId", "appointmentNbr");
            truckVisitAppointment.associateTranAppointment(appointmentEntity);
            PersistenceInterface.getInstance().saveOrUpdate(newUnitEntity);
            PersistenceInterface.getInstance().saveOrUpdate(truckVisitAppointment);

        }
        PersistenceInterface.getInstance().flush();

        return "Success";
    }

    private static DateTimeFormatter localDateFormat = DateTimeFormatter.ofPattern("yy-MMM-dd");

    private static final List<AppointmentStateEnum> INACTIVE_APPT_STATUS = new ArrayList<>() {{
        add(AppointmentStateEnum.USED);
        add(AppointmentStateEnum.EXPIRED);
    }};

    private static final List<AppointmentStateEnum> ACTIVE_APPT_STATUS = new ArrayList<>() {{
        add(AppointmentStateEnum.ACCEPTED);
        add(AppointmentStateEnum.UPDATED);
    }};

    public List<AppointmentEntity> findAppointmentUsingSearchLiteral(String searcStringLiteral, UserEntity userEntity) {
        return appointmentDAO.findAppointmentUsingSearchLiteral(searcStringLiteral, userEntity);
    }

    public List<AppointmentCountDTO> getFutureDaysAppointment(String finalDate) {
        List<AppointmentCountDTO> countDTOList = new ArrayList<AppointmentCountDTO>();
        List<Object[]> list = appointmentDAO.getFutureDaysAppointment(String.valueOf(finalDate));
        for (Object[] value : list) {
            AppointmentCountDTO apptCountDTO = new AppointmentCountDTO();
            if (value[0] != null) {
                if (countDTOList.size() > 0) {
                    int i = 0;
                    for (ListIterator<AppointmentCountDTO> countList = countDTOList.listIterator(); countList.hasNext(); i++) {
                        AppointmentCountDTO appointmentCountDTO = countList.next();
                        if (appointmentCountDTO.getDate().equals(XmlUtil.convertLocalDateToDate((LocalDate) value[1]))) {
                            if (TranTypeEnum.DI.equals(value[0])) {
                                appointmentCountDTO.setImp(value[2].toString());
                            } else if (TranTypeEnum.RE.equals(value[0])) {
                                appointmentCountDTO.setExp(value[2].toString());
                            } else if (TranTypeEnum.RM.equals(value[0])) {
                                appointmentCountDTO.setEmpty(value[2].toString());
                            }
                            break;
                        } else if (countDTOList.size() == i + 1) {
                            AppointmentCountDTO newCountDto = new AppointmentCountDTO();
                            newCountDto.setDate(XmlUtil.convertLocalDateToDate((LocalDate) value[1]));
                            if (TranTypeEnum.DI.equals(value[0])) {
                                newCountDto.setImp(value[2].toString());
                            } else if (TranTypeEnum.RE.equals(value[0])) {
                                newCountDto.setExp(value[2].toString());
                            } else if (TranTypeEnum.RM.equals(value[0])) {
                                newCountDto.setEmpty(value[2].toString());
                            }
                            countDTOList.add(newCountDto);
                            break;
                        }
                    }
                } else {
                    apptCountDTO.setDate(XmlUtil.convertLocalDateToDate((LocalDate) value[1]));
                    if (TranTypeEnum.DI.equals(value[0])) {
                        apptCountDTO.setImp(value[2].toString());
                    } else if (TranTypeEnum.RE.equals(value[0])) {
                        apptCountDTO.setExp(value[2].toString());
                    } else if (TranTypeEnum.RM.equals(value[0])) {
                        apptCountDTO.setEmpty(value[2].toString());
                    }
                    countDTOList.add(apptCountDTO);
                }
            }
        }
        return countDTOList;
    }

    public List<AppointmentDTO> getImportAppointmentForDual() {
        List<AppointmentEntity> appointmentEntityList = appointmentDAO.getImportAppointmentForDual(ACTIVE_APPT_STATUS);
        Set<AppointmentDTO> appointmentDTOList = new HashSet<>();
        for (AppointmentEntity appointmentEntity : appointmentEntityList) {
            AppointmentDTO appointmentDTO = new AppointmentDTO();
            appointmentDTO.setApptTime(appointmentEntity.getGapptTimeSlot().getStartTime());
            appointmentDTO.setApptTranType(appointmentEntity.getApptTranType());
            appointmentDTO.setStatus(appointmentEntity.getApptStatus());
            if (appointmentEntity.getApptExtNbr() != null) {
                appointmentDTO.setApptExtNbr(Long.valueOf(appointmentEntity.getApptExtNbr()));
            }
            if (appointmentEntity.getApptUnit() != null) {
                appointmentDTO.setUnitNbr(appointmentEntity.getApptUnit().getUnitNbr());
            }
            if (appointmentEntity.getGapptTruck() != null) {
                appointmentDTO.setTruckKey(appointmentEntity.getGapptTruck().getTransactionObjectId());
                appointmentDTO.setTruckLicenseNbr(appointmentEntity.getGapptTruck().getTrucklicensenbr());
            } else {
                appointmentDTO.setTruckLicenseNbr("N/A");
            }
            if (appointmentEntity.getId() != null) {
                appointmentDTO.setApptKey(appointmentEntity.getId());
            }
            if (appointmentEntity.getGapptTruckVisitAppt() != null) {
                appointmentDTO.setTvApptKey(appointmentEntity.getGapptTruckVisitAppt().getTransactionObjectId());
            }
            if (appointmentEntity.getGapptTimeSlot() != null) {
                appointmentDTO.setTimeSlotKey(appointmentEntity.getGapptTimeSlot().getTransactionObjectId());
            }
            appointmentDTOList.add(appointmentDTO);
        }
        return List.copyOf(appointmentDTOList);
    }

    public AppointmentEntity getApptByNbr(String apptNbr, UserEntity userEntity) {
        return appointmentDAO.getApptEntityByNbr(apptNbr, userEntity);
    }

    private static final Logger logger = LoggerFactory.getLogger(AppointmentService.class);
}
