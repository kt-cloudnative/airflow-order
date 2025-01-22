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
                , WORKFLOW_NAME='PP_CSNG666B',WORKFLOW_ID='f90657ad0db74b7ca56db240374ed762', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG666B-0.0.prd-tz.8.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f90657ad0db74b7ca56db240374ed762')

    csng666bJob_vol = []
    csng666bJob_volMnt = []
    csng666bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng666bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng666bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng666bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng666bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng666bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '55173ff8352449c9bcf0034ab7ba402f',
        'volumes': csng666bJob_vol,
        'volume_mounts': csng666bJob_volMnt,
        'env_from':csng666bJob_env,
        'task_id':'csng666bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng666bJob", "fromDate=${YYYYMMDD}", "toDate=${YYYYMMDD}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f90657ad0db74b7ca56db240374ed762')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng666bJob,
        Complete
    ]) 

    # authCheck >> csng666bJob >> Complete
    workflow








