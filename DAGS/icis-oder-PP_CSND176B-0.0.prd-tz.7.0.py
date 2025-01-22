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
                , WORKFLOW_NAME='PP_CSND176B',WORKFLOW_ID='76954761ce21446f8d2542b18cd7db2c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND176B-0.0.prd-tz.7.0'
    ,'schedule_interval':'00 21 * * *'
    ,'start_date': datetime(2024, 7, 24, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('76954761ce21446f8d2542b18cd7db2c')

    csnd176bJob_vol = []
    csnd176bJob_volMnt = []
    csnd176bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnd176bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnd176bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd176bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd176bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd176bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ffe7f944a3b946d3a8a7920697ddc9ea',
        'volumes': csnd176bJob_vol,
        'volume_mounts': csnd176bJob_volMnt,
        'env_from':csnd176bJob_env,
        'task_id':'csnd176bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.139',
        'arguments':["--job.name=csnd176bJob", "endTranDate=${YYYYMMDD}", "pgmNm=csnd176b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('76954761ce21446f8d2542b18cd7db2c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd176bJob,
        Complete
    ]) 

    # authCheck >> csnd176bJob >> Complete
    workflow








