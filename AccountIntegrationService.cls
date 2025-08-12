public without sharing class AccountIntegrationService {
    public AccountIntegrationService(){}
    
    public void initIntegrations(Account accountItem){
        // Check available queueable jobs before enqueuing
        if(System.Limits.getQueueableJobs() < System.Limits.getLimitQueueableJobs()) {
            Id jobId = System.enqueueJob(new IntegrationQueueable(accountItem));
        } else {
            // If no queueable jobs available, run synchronously
            runIntegrationsSync(accountItem);
        }
    }

    public List<Account> getAccountData(Set<String> accIdSet){
        return [SELECT Id,DNB_Interface_Status__c,BDI_Interface_Status__c,BlackListInterfaceStatus__c,
                Get_Info_for_Children__c, External_Id__c
                FROM Account WHERE Id =: accIdSet];
    }

    // New unified queueable class that handles all integrations in sequence
    public class IntegrationQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        
        public IntegrationQueueable(Account accountItem) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
        }
        
        public void execute(QueueableContext qc) {
            try {
                // Step 1: DNB Integration
                if(this.accountItem.DNB_Interface_Status__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing dnb ' + this.accountItem);
                    Utils_DandB.getDandBForAccountId(accountItem);
                    System.debug('dnb finish');
                }

                // Refresh account data after DNB call
                AccountIntegrationService srv = new AccountIntegrationService();
                this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];

                // Step 2: BDI Integration
                if(this.accountItem.BDI_Interface_Status__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing bdi ' + this.accountItem);
                    Utils_BDI.getBDIForAccountId(accountItem);
                    System.debug('bdi finish');
                }

                // Refresh account data after BDI call
                this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];

                // Step 3: CNET Integration
                if(this.accountItem.BlackListInterfaceStatus__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing cnet ' + this.accountItem);
                    CnetService service = new CnetService();
                    Account accToUpdate = service.searchWSResponse(accountItem.Id);
                    if (accToUpdate != null) {
                        service.executeDML();
                        System.debug('cnet executed='+accToUpdate);
                    }
                }

                // Step 4: Children Processing
                if(this.accountItem.Get_Info_for_Children__c == true) {
                    processChildren();
                }
                
            } catch(Exception e) {
                System.debug(LoggingLevel.ERROR, 'Error in IntegrationQueueable: ' + e.getMessage());
                // Log error or send notification as needed
            }
        }
        
        private void processChildren() {
            System.debug('calculate kids');
            List<String> fieldsToSetPending = new List<String>{'DNB_Interface_Status__c','BDI_Interface_Status__c','BlackListInterfaceStatus__c'};
            
            Map<Id, Owners_By_Register__c> ownersByRegistersList = new Map<Id, Owners_By_Register__c>([
                SELECT Id, OwnerIDNum__c 
                FROM Owners_By_Register__c 
                WHERE RecordType.DeveloperName = 'D_B_Owners_By_Register' 
                AND Credit_Information__r.Account__c = :accountItem.Id
            ]);
            Set<String> ownerNumSet = new Set<String>();

            System.debug('###account ext id='+accountItem.External_Id__c);
            for (Owners_By_Register__c ownersByRegister : ownersByRegistersList.values()) {
                System.debug('###owner by reg id='+ownersByRegister.OwnerIDNum__c);
                if (ownersByRegister.OwnerIDNum__c != accountItem.External_Id__c) {
                    ownerNumSet.add(ownersByRegister.OwnerIDNum__c);
                }
            }

            Map<Id, Account> childAccounts = new Map<Id, Account>([
                SELECT Id, External_Id__c,BlackListInterfaceStatus__c,BDI_Interface_Status__c, DNB_Interface_Status__c 
                FROM Account 
                WHERE External_Id__c IN :ownerNumSet
            ]);
            
            if(!childAccounts.isEmpty()) {
                AccountIntegrationChildBatch childBatch = new AccountIntegrationChildBatch(childAccounts.keySet(), this.accountItem, fieldsToSetPending);
                Database.executeBatch(childBatch,1);
            }
        }
    }

    // Synchronous fallback method when queueable limit is reached
    public void runIntegrationsSync(Account accountItem) {
        try {
            AccountIntegrationService srv = new AccountIntegrationService();
            Account currentAccount = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            
            // Run DNB synchronously
            if(currentAccount.DNB_Interface_Status__c == 'Pending'){
                callBNDSync(currentAccount);
            }
            
            // Refresh and run BDI
            currentAccount = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            if(currentAccount.BDI_Interface_Status__c == 'Pending'){
                callBDISync(currentAccount);
            }
            
            // Refresh and run BlackList
            currentAccount = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            if(currentAccount.BlackListInterfaceStatus__c == 'Pending'){
                callBlackListSync(currentAccount);
            }
            
            System.debug('Integrations completed synchronously due to queueable job limits');
            
        } catch(Exception e) {
            System.debug(LoggingLevel.ERROR, 'Error in runIntegrationsSync: ' + e.getMessage());
        }
    }

    // Legacy queueable classes - keeping for backward compatibility but not using in new flow
    public class SendBNDQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        Boolean goToOtherQueueable;
        public SendBNDQueueable(Account accountItem, Boolean goToOtherQueueable) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.goToOtherQueueable = goToOtherQueueable;
        }
        public void execute(QueueableContext qc) {
            System.debug(LoggingLevel.INFO,'### executing dnb ' + this.accountItem);
            if(this.accountItem.DNB_Interface_Status__c == 'Pending'){
                Utils_DandB.getDandBForAccountId(accountItem);
                System.debug('bnd finish');
            }

            goToOtherQueueable = Test.isRunningTest()? false : goToOtherQueueable;
            if(goToOtherQueueable == true) {
                System.debug('moving to bdi from bnd');
                if(!Test.isRunningTest()){
                	Id jobId = System.enqueueJob(new SendBDIQueueable(accountItem, goToOtherQueueable));
                }
            }
        }
    }

    public class SendBDIQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        Boolean goToOtherQueueable;

        public SendBDIQueueable(Account accountItem, Boolean goToOtherQueueable) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.goToOtherQueueable = goToOtherQueueable;
        }
        public void execute(QueueableContext param1) {
            System.debug(LoggingLevel.INFO,'### executing bdi ' + this.accountItem);
            if(this.accountItem.BDI_Interface_Status__c == 'Pending'){
                Utils_BDI.getBDIForAccountId(accountItem);
                System.debug('bdi finish');
            }

            goToOtherQueueable = Test.isRunningTest()? false : goToOtherQueueable;
            if(goToOtherQueueable) {
                System.debug('moving to cnet from bdi');
                Id jobId = System.enqueueJob(new SendToCnetQueueable(accountItem, goToOtherQueueable));
            }
        }
    }

    public class SendToCnetQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        Boolean goToOtherQueueable;
        private List<String> fieldsToSetPending = new List<String>{'DNB_Interface_Status__c','BDI_Interface_Status__c','BlackListInterfaceStatus__c'};

        public SendToCnetQueueable(Account accountItem, Boolean goToOtherQueueable) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.goToOtherQueueable = goToOtherQueueable;
        }

        public void execute(QueueableContext context) {
            System.debug(LoggingLevel.INFO,'### executing cnet ' + this.accountItem);

            if(this.accountItem.BlackListInterfaceStatus__c == 'Pending'){
                CnetService service = new CnetService();
                Account accToUpdate = service.searchWSResponse(accountItem.Id);
                if (accToUpdate != null) {
                    service.executeDML();
                    System.debug('cnet executed='+accToUpdate);
                }
            }

            if(this.accountItem.Get_Info_for_Children__c == true) {
                System.debug('calculate kids');
                //children batch
                Map<Id, Owners_By_Register__c> ownersByRegistersList = new Map<Id, Owners_By_Register__c>([SELECT Id, OwnerIDNum__c FROM Owners_By_Register__c WHERE RecordType.DeveloperName = 'D_B_Owners_By_Register' AND Credit_Information__r.Account__c = :accountItem.Id]);
                Set<String> ownerNumSet = new Set<String>();


                System.debug('###account ext id='+accountItem.External_Id__c);
                for (Owners_By_Register__c ownersByRegister : ownersByRegistersList.values()) {
                    System.debug('###owner by reg id='+ownersByRegister.OwnerIDNum__c);
                    if (ownersByRegister.OwnerIDNum__c != accountItem.External_Id__c) {
                        ownerNumSet.add(ownersByRegister.OwnerIDNum__c);
                    }
                }

                Map<Id, Account> childAccounts = new Map<Id, Account>([SELECT Id, External_Id__c,BlackListInterfaceStatus__c,BDI_Interface_Status__c, DNB_Interface_Status__c FROM Account WHERE External_Id__c IN :ownerNumSet]);
                AccountIntegrationChildBatch childBatch = new AccountIntegrationChildBatch(childAccounts.keySet(), this.accountItem, this.fieldsToSetPending);
                Database.executeBatch(childBatch,1);
            }
        }
    }


    public void callBNDSync(Account accountItem){
        Utils_DandB.getDandBForAccountId(accountItem);
        System.debug('bnd finish');
    }

    public void callBDISync(Account accountItem){
        Utils_BDI.getBDIForAccountId(accountItem);
        System.debug('bdi finish');
    }

    public void callBlackListSync(Account accountItem){
        CnetService service = new CnetService();
        Account accToUpdate = service.searchWSResponse(accountItem.Id);
        if (accToUpdate != null) {
            service.executeDML();
            System.debug('cnet executed='+accToUpdate);
        }
    }

}