from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='ET_CBOG042B',WORKFLOW_ID='1b7956c5a3264145b2f22e45405d6ca7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG042B-0.4.prd-tz.1.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 48, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1b7956c5a3264145b2f22e45405d6ca7')

    cbog042bJob_vol = []
    cbog042bJob_volMnt = []
    cbog042bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog042bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog042bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog042bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog042bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog042bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e58507d84f51423a9b814a80b91129dc',
        'volumes': cbog042bJob_vol,
        'volume_mounts': cbog042bJob_volMnt,
        'env_from':cbog042bJob_env,
        'task_id':'cbog042bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=cbog042bJob", "gvcInputFile= kt_intbill_normal_202412.txt", "gvcNewFlag=1", "gvcMobileFlag=2", "gvcInDelFlag=N", "gvcInErrDelFlag=N", "gvcInCustNoFlag=N", "gvcImsiCustFlag=Y", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1b7956c5a3264145b2f22e45405d6ca7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog042bJob,
        Complete
    ]) 

    # authCheck >> cbog042bJob >> Complete
    workflow








