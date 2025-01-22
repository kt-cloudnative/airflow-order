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
                , WORKFLOW_NAME='PP_CSND269B',WORKFLOW_ID='4314c2dd18b14c06bf5560b84befd20e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND269B-0.0.prd-tz.4.0'
    ,'schedule_interval':'00 21 * * *'
    ,'start_date': datetime(2024, 7, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4314c2dd18b14c06bf5560b84befd20e')

    csnd269bJob_vol = []
    csnd269bJob_volMnt = []
    csnd269bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnd269bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnd269bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd269bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd269bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd269bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '328c8df2f05c46d1abfbf67d9478a369',
        'volumes': csnd269bJob_vol,
        'volume_mounts': csnd269bJob_volMnt,
        'env_from':csnd269bJob_env,
        'task_id':'csnd269bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.133',
        'arguments':["--job.name=csnd269bJob", "endTranDate=${YYYYMMDD}", "pgmNm=csnd269b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4314c2dd18b14c06bf5560b84befd20e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd269bJob,
        Complete
    ]) 

    # authCheck >> csnd269bJob >> Complete
    workflow








